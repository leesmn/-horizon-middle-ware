package com.rocket.queue.service.impl;

import com.rocket.queue.rocket.MqConfig;
import com.rocket.queue.service.FeePlatMqService;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.UUID;

@Service
public class FeePlatMqServiceImpl implements FeePlatMqService {

    @Resource
    MqConfig mqConfig;


    @Resource
    @Qualifier("defaultMQProducer")
    private DefaultMQProducer defaultMQProducer;

    @Override
    public SendResult openAccountMsg(String msgInfo) {
        // 可以不使用Config中的Group
        //defaultMQProducer.setProducerGroup(paramConfigService.feePlatGroup);
        SendResult sendResult = null;
        try {
//            Message sendMsg = new Message(paramConfigService.feePlatTopic,
//                                          paramConfigService.feeAccountTag,
//                                         "fee_open_account_key", msgInfo.getBytes());
//            sendResult = defaultMQProducer.send(sendMsg);
            String str = "Hello RocketMQ!------" + UUID.randomUUID().toString();
            Message msg = new Message(mqConfig.topic1,msgInfo.getBytes());
            try{
                defaultMQProducer.send(msg);
            } catch(MQClientException | RemotingException | MQBrokerException | InterruptedException e){
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sendResult ;
    }
}
