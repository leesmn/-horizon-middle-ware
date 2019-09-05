package com.rocket.queue.rocket;

import com.rocket.queue.transcation.TransactionCheckListenerImpl;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.concurrent.*;

/**
 * RocketMQ 生产者配置
 */
@Configuration
public class ProducerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerConfig.class) ;


    @Resource
    MqConfig mqConfig;

    @Resource
    TransactionCheckListenerImpl transactionCheckListener;

    @Bean(value = "defaultMQProducer")
    public DefaultMQProducer getRocketMQProducer() {
        DefaultMQProducer producer;
        producer = new DefaultMQProducer(mqConfig.groupNameP);
        producer.setNamesrvAddr(mqConfig.namesrvAddrP);
        //如果需要同一个jvm中不同的producer往不同的mq集群发送消息，需要设置不同的instanceName
        if(mqConfig.maxMessageSize!=null){
            producer.setMaxMessageSize(mqConfig.maxMessageSize);
        }
        if(mqConfig.sendMsgTimeout!=null){
            producer.setSendMsgTimeout(mqConfig.sendMsgTimeout);
        }
        //如果发送消息失败，设置重试次数，默认为2次
        if(mqConfig.retryTimesWhenSendFailed!=null){
            producer.setRetryTimesWhenSendFailed(mqConfig.retryTimesWhenSendFailed);
        }
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        return producer;
    }



    /**
     * RocketMQ的事务消息发送使用二阶段提交思路，首先，在消息发送时，先发送消息类型为Prepread类型的消息，
     * 然后在将该消息成功存入到消息服务器后，会回调   TransactionListener#executeLocalTransaction，
     * 执行本地事务状态回调函数，然后根据该方法的返回值，结束事务：
     *    1、COMMIT_MESSAGE ：提交事务。
     *    2、ROLLBACK_MESSAGE：回滚事务。
     *    3、UNKNOW：未知事务状态，此时消息服务器(Broker)收到EndTransaction命令时，将不对这种消息做处理，
     * 消息还处于Prepared类型，存储在主题为：RMQ_SYS_TRANS_HALF_TOPIC的队列中，然后消息发送流程将结束，
     * 那这些消息如何提交或回滚呢？为了实现避免客户端需要再次发送提交、回滚命令，RocketMQ会采取定时任务将
     * RMQ_SYS_TRANS_HALF_TOPIC中的消息取出，然后回到客户端，判断该消息是否需要提交或回滚，来完成事务消息的声明周期
     *
     *
     * 调用链路: 事物消息入口 TransactionMQProducer#sendMessageInTransaction
     *   -> DefaultMQProducerImpl#sendKernelImpl
     *    -> SendMessageProcessor#sendMessage
     *     -> TransactionalMessageServiceImpl#prepareMessage -> parseHalfMessageInner -> store.putMessage
     *    -> DefaultMQProducerImpl#sendMessageInTransaction
     *
     *  消息回查: TransactionalMessageCheckService#onWaitEnd
     *           ->
     */
    @Bean(value = "transactionMQProducer")
    public TransactionMQProducer getTransactionMQProducer(){
        TransactionMQProducer txproducer = new TransactionMQProducer(mqConfig.transactionGroupName);

        txproducer.setNamesrvAddr(mqConfig.namesrvAddrP);
        //如果需要同一个jvm中不同的producer往不同的mq集群发送消息，需要设置不同的instanceName
        if(mqConfig.maxMessageSize!=null){
            txproducer.setMaxMessageSize(mqConfig.maxMessageSize);
        }
        if(mqConfig.sendMsgTimeout!=null){
            txproducer.setSendMsgTimeout(mqConfig.sendMsgTimeout);
        }
        //如果发送消息失败，设置重试次数，默认为2次
        if(mqConfig.retryTimesWhenSendFailed!=null){
            txproducer.setRetryTimesWhenSendFailed(mqConfig.retryTimesWhenSendFailed);
        }

        // 队列数

        try {
            ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("client-transaction-msg-check-thread");
                    return thread;
                }
            });

            /**
             * 第一次提交到消息服务器，消息的主题被替换为RMQ_SYS_TRANS_HALF_TOPIC，当执行本地事务，
             * 如果返回本地事务状态为UN_KNOW时，第二次提交到服务器时将不会做任何操作，也就是消息还存在与RMQ_SYS_TRANS_HALF_TOPIC主题中，
             * 并不能被消息消费者消费，那这些消息最终如何被提交或回滚呢？
             *
             * RocketMQ使用TransactionalMessageCheckService线程定时去检测RMQ_SYS_TRANS_HALF_TOPIC主题中的消息，
             * 回查消息的事务状态。TransactionalMessageCheckService的检测频率默认1分钟，
             * 可通过在broker.conf文件中设置transactionCheckInterval的值来改变默认值，单位为毫秒。
             *
             */
            txproducer.setExecutorService(executorService);
            txproducer.setTransactionListener(transactionCheckListener);
            txproducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        return txproducer;
    }
}
