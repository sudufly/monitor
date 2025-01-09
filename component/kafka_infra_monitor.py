# coding:utf-8
import time
import traceback
from collections import defaultdict

from kafka import KafkaAdminClient, TopicPartition, errors
from kafka.errors import kafka_errors
from kafka.protocol.offset import OffsetResetStrategy, OffsetRequest, OffsetResponse
from urllib3.packages.six import iteritems

from common import common as cm
from config.config import Config
from wx_client import WxClient


class MonitorKafkaInfra(object):
    service = 'kafka消费组监控'
    """Reference:
    https://github.com/DataDog/integrations-core/pull/2730/files
    https://github.com/dpkp/kafka-python/issues/1673
    https://github.com/dpkp/kafka-python/issues/1501
    """
    kafka_admin_client = None
    config = Config()
    wx = WxClient()

    def __init__(self):
        bootstrapServers = self.config.get_bootstrap_servers()
        print bootstrapServers
        self.kafka_admin_client = KafkaAdminClient(bootstrap_servers=bootstrapServers)

    @classmethod
    def get_highwater_offsets(self, kafka_admin_client, topics=None):
        """Fetch highwater offsets for topic_partitions in the Kafka cluster.
        Do this for all partitions in the cluster because even if it has no
        consumers, we may want to measure whether producers are successfully
        producing. No need to limit this for performance because fetching
        broker offsets from Kafka is a relatively inexpensive operation.

        Internal Kafka topics like __consumer_offsets are excluded.
        Sends one OffsetRequest per broker to get offsets for all partitions
        where that broker is the leader:
        https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)

        Arguments:
            topics (set): The set of topics (as strings) for which to fetch
                          highwater offsets. If set to None, will fetch highwater offsets
                          for all topics in the cluster.
        """

        highwater_offsets = {}
        topic_partitions_without_a_leader = set()
        # No sense fetching highwatever offsets for internal topics
        internal_topics = {
            '__consumer_offsets',
            '__transaction_state',
            '_schema',  # Confluent registry topic
        }

        for broker in kafka_admin_client._client.cluster.brokers():
            broker_led_partitions = kafka_admin_client._client.cluster.partitions_for_broker(broker.nodeId)
            # Take the partitions for which this broker is the leader and group
            # them by topic in order to construct the OffsetRequest.
            # Any partitions that don't currently have a leader will be skipped.
            partitions_grouped_by_topic = defaultdict(list)
            if broker_led_partitions is None:
                continue
            for topic, partition in broker_led_partitions:
                if topic in internal_topics or (topics is not None and topic not in topics):
                    continue
                partitions_grouped_by_topic[topic].append(partition)

            # Construct the OffsetRequest
            max_offsets = 1
            request = OffsetRequest[0](
                replica_id=-1,
                topics=[
                    (topic, [(partition, OffsetResetStrategy.LATEST, max_offsets) for partition in partitions])
                    for topic, partitions in iteritems(partitions_grouped_by_topic)])

            # For version >= 1.4.7, I find the ver 1.4.7 _send_request_to_node was changed
            future = kafka_admin_client._send_request_to_node(node_id=broker.nodeId, request=request)
            kafka_admin_client._client.poll(future=future)
            response = future.value

            offsets, unled = self._process_highwater_offsets(response)
            highwater_offsets.update(offsets)
            topic_partitions_without_a_leader.update(unled)

        return highwater_offsets, topic_partitions_without_a_leader

    @classmethod
    def _process_highwater_offsets(self, response):
        """Convert OffsetFetchResponse to a dictionary of offsets.

            Returns: A dictionary with TopicPartition keys and integer offsets:
                    {TopicPartition: offset}. Also returns a set of TopicPartitions
                    without a leader.
        """
        highwater_offsets = {}
        topic_partitions_without_a_leader = set()

        assert isinstance(response, OffsetResponse[0])

        for topic, partitions_data in response.topics:
            for partition, error_code, offsets in partitions_data:
                topic_partition = TopicPartition(topic, partition)
                error_type = errors.for_code(error_code)
                if error_type is errors.NoError:
                    highwater_offsets[topic_partition] = offsets[0]
                # Valid error codes:
                # https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-PossibleErrorCodes.2
                elif error_type is kafka_errors.NotLeaderForPartitionError:
                    topic_partitions_without_a_leader.add(topic_partition)
                elif error_type is kafka_errors.UnknownTopicOrPartitionError:
                    pass
                else:
                    raise error_type("Unexpected error encountered while "
                                     "attempting to fetch the highwater offsets for topic: "
                                     "%s, partition: %s." % (topic, partition))
        assert topic_partitions_without_a_leader.isdisjoint(highwater_offsets)
        return highwater_offsets, topic_partitions_without_a_leader

    @classmethod
    def get_kafka_consumer_offsets(self, kafka_admin_client, consumer_groups=None):
        """Fetch Consumer Group offsets from Kafka.
        Also fetch consumer_groups, topics, and partitions if not
        already specified in consumer_groups.
        Arguments:
            consumer_groups (dict): The consumer groups, topics, and partitions
                for which you want to fetch offsets. If consumer_groups is
                None, will fetch offsets for all consumer_groups. For examples
                of what this dict can look like, see
                _validate_explicit_consumer_groups().

        Returns:
            dict: {(consumer_group, topic, partition): consumer_offset} where
                consumer_offset is an integer.
        """
        consumer_offsets = {}
        old_broker = kafka_admin_client.config['api_version'] < (0, 10, 2)
        if consumer_groups is None:  # None signals to fetch all from Kafka
            if old_broker:
                # raise BadKafkaConsumerConfiguration(WARNING_BROKER_LESS_THAN_0_10_2)
                print "WARNING_BROKER_LESS_THAN_0_10_2"
            for broker in kafka_admin_client._client.cluster.brokers():
                for consumer_group, group_type in kafka_admin_client.list_consumer_groups(broker_ids=[broker.nodeId]):
                    # consumer groups from Kafka < 0.9 that store their offset
                    # in Kafka don't use Kafka for group-coordination so
                    # group_type is empty
                    if group_type in ('consumer', ''):
                        # Typically the consumer group offset fetch sequence is:
                        # 1. For each broker in the cluster, send a ListGroupsRequest
                        # 2. For each consumer group, send a FindGroupCoordinatorRequest
                        # 3. Query the group coordinator for the consumer's offsets.
                        # However, since Kafka brokers only include consumer
                        # groups in their ListGroupsResponse when they are the
                        # coordinator for that group, we can skip the
                        # FindGroupCoordinatorRequest.
                        this_group_offsets = kafka_admin_client.list_consumer_group_offsets(
                            group_id=consumer_group, group_coordinator_id=broker.nodeId)
                        for (topic, partition), (offset, metadata) in iteritems(this_group_offsets):
                            key = (consumer_group, topic, partition)
                            consumer_offsets[key] = offset
        else:
            for consumer_group, topics in iteritems(consumer_groups):
                if topics is None:
                    if old_broker:
                        # raise BadKafkaConsumerConfiguration(WARNING_BROKER_LESS_THAN_0_10_2)
                        print "WARNING_BROKER_LESS_THAN_0_10_2"
                    topic_partitions = None
                else:
                    topic_partitions = []
                    # transform from [("t1", [1, 2])] to [TopicPartition("t1", 1), TopicPartition("t1", 2)]
                    for topic, partitions in iteritems(topics):
                        if partitions is None:
                            # If partitions aren't specified, fetch all
                            # partitions in the topic from Kafka
                            partitions = kafka_admin_client._client.cluster.partitions_for_topic(topic)
                        topic_partitions.extend([TopicPartition(topic, p) for p in partitions])
                this_group_offsets = kafka_admin_client.list_consumer_group_offsets(consumer_group,
                                                                                    partitions=topic_partitions)
                for (topic, partition), (offset, metadata) in iteritems(this_group_offsets):
                    # when we are explicitly specifying partitions, the offset
                    # could returned as -1, meaning there is no recorded offset
                    # for that partition... for example, if the partition
                    # doesn't exist in the cluster. So ignore it.
                    if offset != -1:
                        key = (consumer_group, topic, partition)
                        consumer_offsets[key] = offset

        return consumer_offsets

    def monitor(self):
        self.config.get_warning_offsets()
        self.monitor_lag()

    alarm_map = {}

    def monitor_lag(self):
        try:
            warning_offsets = self.config.get_warning_offsets()
            warning_interval = self.config.get_warning_interval()
            alarmMap = self.alarm_map
            gid_set = self.config.get_black_groupid_set()
            lag_map = {}
            cur_time = time.time()

            # {TopicPartition(topic=u'online-events', partition=49): (314735, u'illidan-c')}
            consumer_offsets_dict = {}

            # {TopicPartition(topic=u'online-events', partition=48): 314061}
            # kafka最大偏移量
            topic_offsets_dict, _ = MonitorKafkaInfra.get_highwater_offsets(self.kafka_admin_client,
                                                                            None)
            # {(consumer_group, topic, partition): consumer_offset}
            [consumer_offsets_dict.update({key[0]: key[1]})
             for key in MonitorKafkaInfra.get_kafka_consumer_offsets(self.kafka_admin_client).items()
             if (key[0][0] not in gid_set)
             ]
            for e in consumer_offsets_dict.items():
                key, offset = e[0], e[1]
                gid = key[0]
                topic = key[1]
                partition = key[2]
                tp = TopicPartition(topic, partition)
                max_offset = topic_offsets_dict.get(tp, offset)
                lag = max_offset - offset
                lag_map[key] = lag

                if lag > warning_offsets:
                    print("key:{},lag:{}".format(key, lag))
                    info = alarmMap.get(key, {})
                    info['detectTime'] = cur_time
                    alarmMap[key] = info

            # print msgMap
            cur_time = time.time()
            remove_arr = []
            for key in alarmMap:
                info = alarmMap[key]

                lag = lag_map.get(key, 0)
                detectTime = info['detectTime']
                alarmTime = info.get("alarmTime", cur_time)

                msg = "消费组: {}, Topic: {}, 分区: {}, 未消费消息条数: {}".format(
                    gid, topic, partition, lag)

                # 1min 没有检测到则代表恢复
                if cur_time - detectTime > 60:

                    self.wx.send(recover(self.config.get_project(), self.service, alarmTime,
                                         '告警恢复', msg))
                    remove_arr.append(key)

                elif info.has_key("alarmTime"):
                    # 每10min 报一次
                    if cur_time - alarmTime > warning_interval and lag > warning_offsets:
                        alarmTime = info['alarmTime']
                        self.wx.send(generate_markdown(self.config.get_project(), self.service, alarmTime,
                                                       '消费组未消费消息条数>{}'.format(warning_offsets), msg))

                else:
                    info['alarmTime'] = cur_time
                    self.wx.send(generate_markdown(self.config.get_project(), self.service, alarmTime,
                                                   '消费组未消费消息条数>{}'.format(warning_offsets), msg))
                alarmMap[key] = info
            # 遍历要删除的键列表并从字典中删除这些键
            for key in remove_arr:
                del alarmMap[key]
        except Exception as e:
            traceback.print_exc()


def generate_markdown(project, service, alarmTime, content, detail):
    markdown = """
<font color = warning >{project}告警</font>
><font color = info >服务:</font>  {service} 
><font color = info >触发时间:</font>  {alarmtime} 
><font color = info >报警时间:</font>  {timestamp} 
><font color = info >报警内容:</font>  {content} 
><font color = info >报警明细:</font> {detail}
"""

    return markdown.format(
        project=project,
        service=service,
        content=content,
        detail=detail,
        alarmtime=cm.utc_ms_to_time(alarmTime * 1000),
        timestamp=cm.get_time()
    )


def recover(project, service, alarmtime, content, lag):
    markdown = """
<font color = warning >{project}告警 </font><font color = info >[恢复]</font>
><font color = info >服务:</font>  {service} 
><font color = info >触发时间:</font>  {alarmtime} 
><font color = info >恢复时间:</font>  {timestamp} 
><font color = info >报警状态:</font>  {content} 
><font color = info >当前状态:</font>  {lag}
"""
    return markdown.format(
        project=project,
        service=service,
        content=content,
        lag=lag,
        alarmtime=cm.utc_ms_to_time(alarmtime * 1000),
        timestamp=cm.get_time()
    )
