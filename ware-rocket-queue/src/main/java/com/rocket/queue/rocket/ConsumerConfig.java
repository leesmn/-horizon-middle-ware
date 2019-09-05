package com.rocket.queue.rocket;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import javax.annotation.Resource;
/**
 * RocketMQ 消费者配置
 */
@Configuration
public class ConsumerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerConfig.class) ;
    @Resource
    MqConfig mqConfig;

    @Resource
    private RocketMsgListener msgListener;
    @Bean("defaultMQPushConsumer")
    public DefaultMQPushConsumer getRocketMQConsumer(){
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(mqConfig.groupName);
        consumer.setNamesrvAddr(mqConfig.namesrvAddr);
        consumer.setConsumeThreadMin(mqConfig.consumeThreadMin);
        consumer.setConsumeThreadMax(mqConfig.consumeThreadMax);
        consumer.registerMessageListener(msgListener);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeMessageBatchMaxSize(mqConfig.consumeMessageBatchMaxSize);
        try {
            String[] topicTagsArr = mqConfig.topics.split(";");
            for (String topicTags : topicTagsArr) {
                String[] topicTag = topicTags.split("~");
                consumer.subscribe(topicTag[0],topicTag[1]);
            }
            consumer.start();
        }catch (MQClientException e){
            e.printStackTrace();
        }
        return consumer;
    }

    @Bean("transactionMQPushConsumer")
    public DefaultMQPushConsumer getTransactionRocketMQConsumer(){
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(mqConfig.transactionGroupName);
        consumer.setNamesrvAddr(mqConfig.namesrvAddr);
        consumer.setConsumeThreadMin(mqConfig.consumeThreadMin);
        consumer.setConsumeThreadMax(mqConfig.consumeThreadMax);
        consumer.registerMessageListener(msgListener);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeMessageBatchMaxSize(mqConfig.consumeMessageBatchMaxSize);
        try {
            String[] topicTagsArr = mqConfig.transactionTopics.split(";");
            for (String topicTags : topicTagsArr) {
                String[] topicTag = topicTags.split("~");
                consumer.subscribe(topicTag[0],topicTag[1]);
            }
            consumer.start();
        }catch (MQClientException e){
            e.printStackTrace();
        }
        return consumer;
    }
}
