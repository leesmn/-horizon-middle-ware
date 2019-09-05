package com.rocket.queue.service;

import org.apache.rocketmq.client.producer.SendResult;

/**
 * @author lis
 * @description:
 * @date 2019/07/30
 **/
public interface TransactionMqService {
    SendResult sendTransactionMsg (String msgInfo,String tag,String key) ;
}
