package com.rocket.queue.service.impl;

import com.rocket.queue.rocket.MqConfig;
import com.rocket.queue.service.ScheduledMqService;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.Resource;
import java.util.UUID;

/**
 * @author lis
 * @description:
 * @date 2019/07/31
 **/
public class ScheduledMqServiceImpl implements ScheduledMqService {
    @Resource
    MqConfig mqConfig;


    @Resource
    @Qualifier("defaultMQProducer")
    private DefaultMQProducer defaultMQProducer;

    @Override
    public SendResult sendTransactionMsg(String msgInfo, String tag, String key) {
        SendResult sendResult = null;
        try {
            String str = "Hello RocketMQ!------" + UUID.randomUUID().toString();
            Message msg = new Message(mqConfig.topic1,msgInfo.getBytes());
            try{
                defaultMQProducer.send(msg);
                // This message will be delivered to consumer 5 seconds later.
                // messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m
                // 20m 30m 1h 2h
                // 这个配置项配置了从1级开始，各级延时的时间，可以修改这个指定级别的延时时间；
                // 时间单位支持：s、m、h、d，分别表示秒、分、时、天；
                // 默认值就是上面声明的，可手工调整；
                // 默认值已够用，不建议修改这个值。
                msg.setDelayTimeLevel(1);
            } catch(MQClientException | RemotingException | MQBrokerException | InterruptedException e){
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sendResult ;
    }
}
