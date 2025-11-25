#!/bin/bash
####  不支持公网下载则使用shell脚本
set -euo pipefail
path=`pwd`
project=瑞驰
startupTime=$(date +'%Y-%m-%d %H:%M:%S')

# ========================= 配置参数（用户根据实际情况修改）=========================
# Kafka 集群地址（多个用逗号分隔）
KAFKA_HOME="/opt/kafka"
KAFKA_BROKERS="10.254.136.8:9092"
# 企业微信机器人 Webhook 地址（报警和状态通知）
WECHAT_WEBHOOK="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=81682c7f-ef22-4238-8883-912c7df5dcaf"
# 企业微信机器人 Webhook 地址（心跳专用）
HEARTBEAT_WEBHOOK="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=02df9f02-a30b-4f91-b5f5-2cc9d2a134d1"

# 偏移量阈值（超过该值触发报警）
THRESHOLD=50000
# 检测间隔（秒）
CHECK_INTERVAL=30
# 报警间隔（秒，30分钟=1800秒）
ALARM_INTERVAL=1800
# 消费组过滤列表（数组形式，支持多个消费组，默认监控所有则留空数组）
CONSUMER_GROUP_FILTER=()

# 心跳配置
HEARTBEAT_TIME="00:00"

# 缓存文件
mkdir -p ./logs
# 记录当前处于报警状态的分区 (PENDING)
ALARM_CACHE="logs/kafka_alarm_cache.txt"
# 记录已从报警状态恢复的分区 (RESOLVED)
RECOVERY_CACHE="logs/kafka_recovery_cache.txt"
# 记录上次心跳发送时间
LAST_HEARTBEAT_FILE="logs/kafka_heartbeat_last_sent.txt"
# 记录每个消费组:主题的最近一次报警时间
TOPIC_ALARM_CACHE="logs/kafka_topic_alarm_cache.txt"

# ========================= 工具函数 =========================
# 日志打印函数
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

# 通用消息发送函数
send_msg() {
    local webhook_url=$1
    local content=$2
    local masked_url=$(echo "$webhook_url" | sed 's/\(key=\).*/\1****/')

    curl -s -X POST "$webhook_url" \
        -H "Content-Type: application/json" \
        -d "{
            \"msgtype\": \"markdown\",
            \"markdown\": {
                \"content\": \"$content\"
            }
        }" >/dev/null 2>&1

    if [ $? -eq 0 ]; then
        :
    else
        log "失败发送消息到 $masked_url"
    fi
}

# 发送报警消息
send_alarm_msg() {
    local group=$1
    local topic=$2
    local partition=$3
    local lag=$4

    local content="
<font color = warning >${project}告警</font>
><font color = info >服务:</font>  Kafka消费组监控
><font color = info >触发时间:</font>  $(date +'%Y-%m-%d %H:%M:%S')
><font color = info >报警时间:</font>  $(date +'%Y-%m-%d %H:%M:%S')
><font color = info >报警内容:</font>  消费组未消费消息条数>${THRESHOLD}
><font color = info >报警明细:</font> 消费组: $group, Topic: $topic, 分区: $partition, 未消费消息条数: $lag
"
    send_msg "$WECHAT_WEBHOOK" "$content"
}

# 发送恢复消息
send_recovery_msg() {
    local group=$1
    local topic=$2
    local partition=$3
    local lag=$4

    local content="

<font color = warning >${project}告警 </font><font color = info >[恢复]</font>
><font color = info >服务:</font>  Kafka消费组监控
><font color = info >触发时间:</font>  $(date +'%Y-%m-%d %H:%M:%S')
><font color = info >恢复时间:</font>  $(date +'%Y-%m-%d %H:%M:%S')
><font color = info >报警状态:</font>  告警恢复
><font color = info >当前状态:</font>  $group, Topic: $topic, 分区: $partition, 未消费消息条数: $lag
"
    send_msg "$WECHAT_WEBHOOK" "$content"
}

# 发送启动通知
send_startup_msg() {
    local content="
<font color = info >${project}告警服务 [启动]</font>
><font color = info >启动时间:</font>  ${startupTime}
><font color = info >服务说明:</font>  \n shell Kafka消费组监控服务
"
    send_msg "$HEARTBEAT_WEBHOOK" "$content"
}

# 发送退出通知
send_shutdown_msg() {
    local content="
<font color = warning >${project}告警服务 [异常]</font>
><font color = info >启动时间:</font>  ${startupTime}
><font color = info >退出时间:</font>  $(date +'%Y-%m-%d %H:%M:%S')
><font color = info >退出原因:</font> 服务意外终止
"
    send_msg "$HEARTBEAT_WEBHOOK" "$content"
}

# 发送心跳消息
send_heartbeat_msg() {
    local content="
<font color = info >${project}告警服务 [心跳]</font>
><font color = info >启动时间:</font>  ${startupTime}
><font color = info >当前时间:</font>  $(date +'%Y-%m-%d %H:%M:%S')
"
    send_msg "$HEARTBEAT_WEBHOOK" "$content"
    date +'%Y-%m-%d %H:%M:%S' > "$LAST_HEARTBEAT_FILE"
}

# 检查分区是否在报警缓存中 (是否处于报警状态)
is_in_alarm() {
    local group=$1
    local topic=$2
    local partition=$3
    [ -f "$ALARM_CACHE" ] && grep -q "^$group:$topic:$partition$" "$ALARM_CACHE" && return 0
    return 1
}

# 检查分区是否在恢复缓存中 (是否已发送过恢复通知)
is_recovered() {
    local group=$1
    local topic=$2
    local partition=$3
    [ -f "$RECOVERY_CACHE" ] && grep -q "^$group:$topic:$partition$" "$RECOVERY_CACHE" && return 0
    return 1
}

# 将分区标记为报警状态
mark_as_alarm() {
    local group=$1
    local topic=$2
    local partition=$3
    if ! is_in_alarm "$group" "$topic" "$partition"; then
        echo "$group:$topic:$partition" >> "$ALARM_CACHE"
    fi
}

# 将分区标记为恢复状态
mark_as_recovered() {
    local group=$1
    local topic=$2
    local partition=$3
    if ! is_recovered "$group" "$topic" "$partition"; then
        echo "$group:$topic:$partition" >> "$RECOVERY_CACHE"
    fi
}

# 清除分区的报警状态
clear_alarm_mark() {
    local group=$1
    local topic=$2
    local partition=$3
    sed -i "/^$group:$topic:$partition$/d" "$ALARM_CACHE" 2>/dev/null
}

# 清除分区的恢复状态
clear_recovered_mark() {
    local group=$1
    local topic=$2
    local partition=$3
    sed -i "/^$group:$topic:$partition$/d" "$RECOVERY_CACHE" 2>/dev/null
}

# 检查消费组:主题是否在报警间隔内
is_topic_in_alarm_interval() {
    local group=$1
    local topic=$2
    local current_time=$(date +%s)
    local cache_key="$group:$topic"

    if [ -f "$TOPIC_ALARM_CACHE" ]; then
        local cache_entry=$(grep "^$cache_key:" "$TOPIC_ALARM_CACHE")
        if [ -n "$cache_entry" ]; then
            local last_alarm_time=$(echo "$cache_entry" | awk -F ':' '{print $3}')
            local time_diff=$((current_time - last_alarm_time))
            [ $time_diff -lt $ALARM_INTERVAL ] && return 0
        fi
    fi
    return 1
}

# 更新消费组:主题的最近报警时间
update_topic_alarm_cache() {
    local group=$1
    local topic=$2
    local current_time=$(date +%s)
    local cache_key="$group:$topic"

    sed -i "/^$cache_key:/d" "$TOPIC_ALARM_CACHE" 2>/dev/null
    echo "$cache_key:$current_time" >> "$TOPIC_ALARM_CACHE"
}

# 检查消费组是否在过滤列表中
is_group_allowed() {
    local group=$1
    if [ ${#CONSUMER_GROUP_FILTER[@]} -eq 0 ]; then
        return 0
    fi
    for allowed_group in "${CONSUMER_GROUP_FILTER[@]}"; do
        if [ "$allowed_group" = "$group" ]; then
            return 0
        fi
    done
    return 1
}

# 检查是否需要发送心跳
check_heartbeat() {
    if [ ! -f "$LAST_HEARTBEAT_FILE" ] || [ "$(date -f "$LAST_HEARTBEAT_FILE" +'%Y-%m-%d')" != "$(date +'%Y-%m-%d')" ]; then
        local current_time=$(date +'%H:%M')
        if [[ "$current_time" > "$HEARTBEAT_TIME" || "$current_time" == "$HEARTBEAT_TIME" ]]; then
            log "准备发送每日心跳..."
            send_heartbeat_msg
        fi
    fi
}

# ========================= 核心监控逻辑 =========================
main() {
    send_startup_msg
    log "启动Kafka消费组偏移量监控，检测间隔=$CHECK_INTERVAL秒，阈值=$THRESHOLD，报警间隔=$ALARM_INTERVAL秒"
    if [ ${#CONSUMER_GROUP_FILTER[@]} -eq 0 ]; then
        log "消费组过滤列表为空，监控所有消费组"
    else
        log "监控的消费组列表：${CONSUMER_GROUP_FILTER[*]}"
    fi

    while true; do
        log "开始新一轮检测..."
        all_offsets=$($KAFKA_HOME/bin/kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BROKERS" \
            --all-groups --describe 2>/dev/null | \
            awk 'NR == 1 { next } { print $1, $2, $3, $6 }')

        if [ -z "$all_offsets" ]; then
            log "警告：未获取到任何消费组的偏移量数据"
        else
            echo "$all_offsets" | while read -r group topic partition lag; do
                if ! is_group_allowed "$group"; then
                    continue
                fi
                if ! [[ "$lag" =~ ^[0-9]+$ ]]; then
                    log "警告：消费组=$group, 主题=$topic, 分区=$partition 的LAG值'$lag'无效，已跳过"
                    continue
                fi

                # 核心逻辑
                if [ "$lag" -ge "$THRESHOLD" ]; then
                    # 情况1: 分区堆积超过阈值 (应处于报警状态)

                    # 如果之前已恢复，现在再次报警，需要清除恢复标记
                    if is_recovered "$group" "$topic" "$partition"; then
                        clear_recovered_mark "$group" "$topic" "$partition"
                        log "状态变更: 消费组=$group, 主题=$topic, 分区=$partition 从[已恢复]再次进入[报警]状态。"
                    fi

                    # 将其标记为报警状态
                    mark_as_alarm "$group" "$topic" "$partition"

                    # 检查是否需要发送报警通知 (基于Topic的报警间隔)
                    if ! is_topic_in_alarm_interval "$group" "$topic"; then
                        log "触发报警：消费组=$group, 主题=$topic, 分区=$partition, 堆积=$lag"
                        send_alarm_msg "$group" "$topic" "$partition" "$lag"
                        update_topic_alarm_cache "$group" "$topic"
                    else
                        # 在间隔内，不发送报警
                        : # log "已报警（未到间隔）：消费组=$group, 主题=$topic"
                    fi

                else
                    # 情况2: 分区堆积低于阈值 (应处于正常状态)

                    # 只有当它之前处于报警状态时，才发送恢复通知
                    if is_in_alarm "$group" "$topic" "$partition"; then
                        log "触发恢复：消费组=$group, 主题=$topic, 分区=$partition, 堆积=$lag"
                        send_recovery_msg "$group" "$topic" "$partition" "$lag"

                        # 更新状态：清除报警标记，设置恢复标记
                        clear_alarm_mark "$group" "$topic" "$partition"
                        mark_as_recovered "$group" "$topic" "$partition"
                    fi
                    # else:
                    # 情况3: 分区一直处于正常状态，或已恢复且未再次报警，不做任何操作
                fi
            done
        fi

        check_heartbeat
        log "本次检测完成，等待$CHECK_INTERVAL秒后再次检测"
        sleep $CHECK_INTERVAL
    done
}

# ========================= 脚本入口 =========================
trap send_shutdown_msg EXIT

check_dependency() {
    if ! command -v "$1" &> /dev/null; then
        log "错误：未找到依赖工具$1，请先安装"
        exit 1
    fi
}

check_dependency "$KAFKA_HOME/bin/kafka-consumer-groups.sh"
check_dependency "curl"
check_dependency "date"

# 初始化所有缓存文件
touch "$ALARM_CACHE" "$RECOVERY_CACHE" "$LAST_HEARTBEAT_FILE" "$TOPIC_ALARM_CACHE" 2>/dev/null

main