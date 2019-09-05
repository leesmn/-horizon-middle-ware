package com.rocket.queue.controller;

import com.rocket.queue.service.FeePlatMqService;
import com.rocket.queue.service.TransactionMqService;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
public class RocketController {

    @Resource
    private FeePlatMqService feePlatMqService ;

    @Resource
    private TransactionMqService transactionMqService ;

    @RequestMapping("/sendMsg")
    public SendResult sendMsg (){
        String msg = "OpenAccount Msg";
        SendResult sendResult = null;
        try {
            sendResult = feePlatMqService.openAccountMsg(msg) ;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sendResult ;
    }
    @RequestMapping("/sendTrans")
    public void sendTrans(){
        String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
        //TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
        for (int i = 0; i < 3; i++) {
            transactionMqService.sendTransactionMsg("hello rocketmq", tags[i % tags.length], "KEY" + i);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }



    @RequestMapping("/sendTest")
    public void sendTest() {
        // 声明并初始化一个producer
        // 需要一个producer group名字作为构造方法的参数，这里为producer1
        DefaultMQProducer producer = new DefaultMQProducer("producer12");
        producer.setVipChannelEnabled(false);
        // 设置NameServer地址,此处应改为实际NameServer地址，多个地址之间用；分隔
        // NameServer的地址必须有
        // producer.setClientIP("119.23.211.22");
        // producer.setInstanceName("Producer");
        producer.setNamesrvAddr("47.103.130.73:9876");

        // 调用start()方法启动一个producer实例
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        // 发送1条消息到Topic为TopicTest，tag为TagA，消息内容为“Hello RocketMQ”拼接上i的值
        try {
            // 封装消息
            Message msg = new Message("TopicTest",// topic
                    "TagA",// tag
                    ("Hello RocketMQ").getBytes(RemotingHelper.DEFAULT_CHARSET)// body
            );
            // 调用producer的send()方法发送消息
            // 这里调用的是同步的方式，所以会有返回结果
            SendResult sendResult = producer.send(msg);
            // 打印返回结果
            System.out.println(sendResult);

            //发送完消息之后，调用shutdown()方法关闭producer
            System.out.println("send success");
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        producer.shutdown();

    }

//    @RequestMapping("/sendT")
//    private void test1(){
////        //生成Producer
////        DefaultMQProducer producer = new DefaultMQProducer("pro_qch_test");
////        //配置Producer
////        producer.setNamesrvAddr("47.103.130.73:9876");
////        producer.setInstanceName(UUID.randomUUID().toString());
////        //启动Producer
////        try{
////            producer.start();
////        }catch(MQClientException e) {
////            e.printStackTrace();
////            return;
////        }
//
//
//
//
//
//        //生产消息
//        String str = "Hello RocketMQ!------" + UUID.randomUUID().toString();
//        Message msg = new Message("broker-a",str.getBytes());
//        try{
//            defaultMQProducer.send(msg);
//        } catch(MQClientException | RemotingException | MQBrokerException | InterruptedException e){
//            e.printStackTrace();
//            return;
//        }
//
//        //停止Producer
//        //producer.shutdown();
//        System.out.println("[-----------]Success\n");
//    }
}
