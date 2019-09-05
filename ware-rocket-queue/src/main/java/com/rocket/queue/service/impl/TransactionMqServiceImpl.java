package com.rocket.queue.service.impl;

import com.rocket.queue.rocket.MqConfig;
import com.rocket.queue.service.TransactionMqService;
import com.rocket.queue.transcation.TransactionCheckListenerImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Calendar;

/**
 * @author lis
 * @description:
 * @date 2019/07/30
 **/
@Service
public class TransactionMqServiceImpl implements TransactionMqService {
    @Resource
    @Qualifier("transactionMQProducer")
    private TransactionMQProducer transactionMQProducer;

    @Resource
    MqConfig mqConfig;

    @Override
    public SendResult sendTransactionMsg(String msgInfo,String tag,String key) {
        TransactionSendResult sendResult = null;
        String time = DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd'T'HH:mm:ss");
        try {
            Message msg = new Message(mqConfig.topic2, tag, key, (time + ":" + msgInfo).getBytes());
            sendResult = transactionMQProducer.sendMessageInTransaction(msg, null);
            System.out.println(sendResult);


            Thread.sleep(10);
        } catch (MQClientException | InterruptedException e) {
            e.printStackTrace();
        }
        return sendResult;
    }
}
