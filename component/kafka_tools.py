# coding:utf-8
import struct
import traceback
from collections import defaultdict
from distutils import errors

from kafka import KafkaAdminClient
from kafka import TopicPartition, errors, KafkaConsumer
from kafka.errors import kafka_errors
from kafka.protocol.offset import OffsetResetStrategy, OffsetRequest, OffsetResponse
from kafka.structs import GroupInformation  # 确保从正确的模块导入
from urllib3.packages.six import iteritems

from config.config import Config


def parse_bytes(data, version=0):
    """Parse byte string to extract metadata or assignment information."""
    if not data:
        return {}

    pos = 0
    result = {}

    # Check version only if provided and greater than 0
    if version > 0:
        if len(data) < 2:
            raise ValueError("Data too short to read version")
        version, = struct.unpack_from('>h', data, pos)
        pos += 2
        result['version'] = version

    # Read topics array length (int32)
    if len(data) - pos < 4:
        raise ValueError("Data too short to read topics array length")
    topics_length, = struct.unpack_from('>i', data, pos)

    pos += 4
    result['topics'] = []

    for _ in range(topics_length):
        # Read topic name length (int16)
        if len(data) - pos < 2:
            raise ValueError("Data too short to read topic name length")
        topic_name_length, = struct.unpack_from('>h', data, pos)
        pos += 2

        # Read topic name (string)
        if len(data) - pos < topic_name_length:
            raise ValueError("Data too short to read topic name")
        topic_name = data[pos:pos + topic_name_length].decode('utf-8')
        pos += topic_name_length

        # Read partition array length (int32)
        if len(data) - pos < 4:
            raise ValueError("Data too short to read partition array length")
        partitions_length, = struct.unpack_from('>i', data, pos)
        pos += 4
        partitions = []

        for __ in range(partitions_length):
            # Read partition id (int32)
            if len(data) - pos < 4:
                raise ValueError("Data too short to read partition id")
            partition_id, = struct.unpack_from('>i', data, pos)
            pos += 4
            partitions.append(partition_id)

        result['topics'].append({
            'topic': topic_name,
            'partitions': partitions
        })

    return result


def parse_describe_groups_response(response_tuple):
    # 使用 isinstance 来检查类型
    if isinstance(response_tuple, GroupInformation):
        print("The object is an instance of GroupInformation.")
    else:
        print("The object is not an instance of GroupInformation.")
    version = 0
    protocol = response_tuple[4]
    print('协议:{}'.format(protocol))
    if protocol == 'range' or protocol == '':
        version = 1
    else:
        version = 0
    error_code, group_id, state, protocol_type, protocol, members = response_tuple
    group_info = {
        'group_id': group_id,
        'error_code': error_code,
        'state': state,
        'protocol_type': protocol_type,
        'protocol': protocol,
        'members': []
    }

    if error_code != 0:
        print("消费组 {}: 错误码 {}".format(group_id, error_code))
        return None

    for member in members:
        member_id, client_id, client_host, member_metadata, member_assignment = member
        parsed_metadata = parse_bytes(member_metadata, version)
        parsed_assignment = parse_bytes(member_assignment, version)

        member_data = {
            'member_id': member_id,
            'client_id': client_id,
            'client_host': client_host,
            'member_metadata': parsed_metadata,
            'member_assignment': parsed_assignment
        }
        group_info['members'].append(member_data)

    return group_info


class KafkaUtil:
    consumer = None
    client = None

    def __init__(self):
        config = Config()
        bootstrap_servers = config.get_bootstrap_servers()
        self.client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',  # 从最早的消息开始
            enable_auto_commit=False  # 禁用自动提交
        )

    # coding:utf-8

    def get_consumer_group_clients(self, group_id):

        # 创建 AdminClient 实例
        admin_client = self.client
        tp_map = {}
        try:
            # 描述指定的消费组
            group_description = admin_client.describe_consumer_groups([group_id])
            group_info = group_description[0]
            if group_info.error_code != 0:
                print("消费组 {} 不存在或无信息".format(group_id).encode('utf-8'))
                return None
            # group_info = parse_describe_groups_response(group_description[0])

            state = group_info.state

            if state != 'Stable':
                print("消费组 {} 不是稳定状态: {}".format(group_id, state))
                return tp_map

            members = group_info.members
            if not members:
                print("消费组 {} 没有活跃成员".format(group_id).encode('utf-8'))
                return tp_map

            client_ids = []

            for member in members:

                client_id = member.client_id.encode('utf-8')
                member_id = member.member_id.encode('utf-8')
                client_host = member.client_host.encode('utf-8')
                client_ids.append({
                    'groupId': group_id,
                    'clientId': client_id,
                    'host': client_host
                })
                member_assignment = (member.member_assignment)
                # print type(member_assignment)
                assignment = member_assignment.assignment
                for tp in assignment:
                    topic = tp[0]
                    partitions = tp[1]
                    for partition in partitions:
                        tp_map[TopicPartition(topic, partition)] = (client_id, member_id, client_host)
                # print("消费组: {}, 客户端ID: {}, 主机: {}".format(group_id, client_id, client_host))

            return tp_map

        except Exception as e:
            print("发生错误: {}".format(e))
            traceback.print_exc()
            return None


    def get_partition_offsets(self, topic_name):
        consumer = self.consumer
        # 创建消费者实例，不订阅任何主题，只用于查询元数据和偏移量

        # 获取所有分区信息
        partitions = consumer.partitions_for_topic(topic_name)
        if not partitions:
            print(u"主题 {} 不存在或没有分区".format(topic_name).encode('utf-8'))
            return None

        # 创建 TopicPartition 对象列表
        tp_list = [TopicPartition(topic_name, p) for p in partitions]

        # 查询每个分区的最早和最新偏移量
        earliest_offsets = None  # consumer.beginning_offsets(tp_list)
        latest_offsets = consumer.end_offsets(tp_list)

        # 输出结果
        offsets_info = {}
        for tp in tp_list:
            earliest_offset = -1
            # earliest_offset = earliest_offsets.get(tp, -1)
            latest_offset = latest_offsets.get(tp, -1)
            # offsets_info.append({
            #     'partition': tp.partition,
            #     'earliest_offset': earliest_offset,
            #     'latest_offset': latest_offset
            # })
            offsets_info[tp] = (earliest_offset, latest_offset)

        consumer.close()
        return offsets_info

    @classmethod
    def get_topic_offsets(self, kafka_admin_client, topics=None):
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
        if type(topics) == str:
            topics = set(topics)
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


    def list_consumer_group_offsets(self,groupId):
        return self.client.list_consumer_group_offsets(groupId)