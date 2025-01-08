# coding:utf-8
import struct
import traceback

from kafka import KafkaAdminClient

from component.kafka_tools import KafkaUtil


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


def get_consumer_group_clients(bootstrap_servers, group_id):
    # 创建 AdminClient 实例
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    try:
        # 描述指定的消费组
        group_description = admin_client.describe_consumer_groups([group_id])
        # print group_description
        group_info = parse_describe_groups_response(group_description[0])
        # print group_info
        if not group_info or len(group_info) == 0:
            print("消费组 {} 不存在或无信息".format(group_id).encode('utf-8'))
            return None

        state = group_info['state']

        if state != 'Stable':
            print("消费组 {} 不是稳定状态: {}".format(group_id, state))
            return None

        members = group_info['members']
        if not members:
            print("消费组 {} 没有活跃成员".format(group_id).encode('utf-8'))
            return None

        client_ids = []
        for member in members:
            client_id = member['client_id'].encode('utf-8')
            client_host = member['client_host'].encode('utf-8')
            client_ids.append({
                'groupId': group_id,
                'clientId': client_id,
                'host': client_host
            })
            print("消费组: {}, 客户端ID: {}, 主机: {}".format(group_id, client_id, client_host))

        return client_ids

    except Exception as e:
        print("发生错误: {}".format(e))
        traceback.print_exc()
        return None
    finally:
        admin_client.close()






if __name__ == "__main__":
    kafka_util = KafkaUtil()
    group_id = 'test-event-consumer7'  # 替换为你想要
    group_id = 'test-event-consumer7'  # 替换为你想要
    # group_id = 'telematics-original'  # 替换为你想要
    # group_id = 'gid-access-data-telematics-store-test-02'  # 替换为你想要
    kafka_util.get_consumer_group_clients(group_id)
