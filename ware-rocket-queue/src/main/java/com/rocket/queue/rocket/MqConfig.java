package com.rocket.queue.rocket;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author lis
 * @description:
 * @date 2019/07/30
 **/
@Configuration
public class MqConfig {
    @Value("${rocketmq.consumer.namesrvAddr}")
    public String namesrvAddr;
    @Value("${rocketmq.consumer.groupName}")
    public String groupName;
    @Value("${rocketmq.consumer.consumeThreadMin}")
    public int consumeThreadMin;
    @Value("${rocketmq.consumer.consumeThreadMax}")
    public int consumeThreadMax;
    @Value("${rocketmq.consumer.topics}")
    public String topics;
    @Value("${rocketmq.consumer.consumeMessageBatchMaxSize}")
    public int consumeMessageBatchMaxSize;


    @Value("${rocketmq.producer.groupName}")
    public String groupNameP;
    @Value("${rocketmq.producer.namesrvAddr}")
    public String namesrvAddrP;
    @Value("${rocketmq.producer.maxMessageSize}")
    public Integer maxMessageSize ;
    @Value("${rocketmq.producer.sendMsgTimeout}")
    public Integer sendMsgTimeout;
    @Value("${rocketmq.producer.retryTimesWhenSendFailed}")
    public Integer retryTimesWhenSendFailed;




    @Value("${transaction-mq.groupName}")
    public String transactionGroupName;
    @Value("${transaction-mq.topics}")
    public String transactionTopics;

    @Value("${send-mq.topic1}")
    public String topic1;
    @Value("${send-mq.topic2}")
    public String topic2;
}
