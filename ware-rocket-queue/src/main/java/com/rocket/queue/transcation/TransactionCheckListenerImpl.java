package com.rocket.queue.transcation;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 未决事务，服务器回查客户端
 */
@Component
public class TransactionCheckListenerImpl implements TransactionListener {
    private AtomicInteger transactionIndex = new AtomicInteger(0);
   // private AtomicInteger executeTransactionIndex = new AtomicInteger(0);
    private ConcurrentHashMap<String,Integer> localTrans = new ConcurrentHashMap<>();
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        int value = transactionIndex.getAndIncrement();
        int status = value % 3;
        localTrans.put(message.getTransactionId(),status);
//        System.out.println("value == " + value);
//        if (value == 0) {
//            throw new RuntimeException("Could not find db");
//        }
//        else if (value == 1) {
//            return LocalTransactionState.ROLLBACK_MESSAGE;
//        }
//        else if (value == 2) {
//            return LocalTransactionState.COMMIT_MESSAGE;
//        }
        System.out.println("TransactionId:"+message.getTransactionId()+",value:" + status);

        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        System.out.println("server checking TrMsg " + messageExt.toString());


        Integer value = localTrans.get(messageExt.getTransactionId());
        if(value!=null){
            if (value == 0) {
                Integer tryCheckTime = Integer.parseInt(messageExt.getProperty("TRANSACTION_CHECK_TIMES"));
                if(tryCheckTime>3){
                   //超过三次取消回查消息的事务
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                throw new RuntimeException("Could not find db");
            }
            else if (value == 1) {
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
            else if (value == 2) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        }


        return LocalTransactionState.UNKNOW;
    }
}
