package com.gupaoedu.example;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 *
 */
public class RocketMqProducer {


    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //事务消息的时候会用到
        DefaultMQProducer producer = new DefaultMQProducer("gp_producer_group");
        producer.setNamesrvAddr("localhost:9876"); //它会从命名服务器上拿到broker的地址
        producer.start();

        int num = 0;
        while (num < 20) {
            num++;
            //Topic
            //tags -> 标签 （分类） -> (筛选)
            Message message = new Message("gp_test_topic", "TagA", ("Hello , RocketMQ:" + num).getBytes());

//            SendResult send = producer.send(message);
//            System.out.println(send);

            //异步发送 需要回值
//            producer.send(message, new SendCallback() {
//                @Override
//                public void onSuccess(SendResult sendResult) {
//
//                }
//
//                @Override
//                public void onException(Throwable throwable) {
//
//                }
//            });

            //消息路由策略
            producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List< MessageQueue > list, Message message, Object o) {
                    System.out.println("路由："+list.size());
                    return list.get(0);
                }
            }, "key-" + num);
        }
    }
}

