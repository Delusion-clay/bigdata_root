package com.cn.wifi.kafka.producer;

import com.cn.wifi.kafka.config.KakfaConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

public class StringProducer {
    private static final Logger LOG = Logger.getLogger(StringProducer.class);

    //发送消息
    /**
     *
     * @param topic
     * @param msg
     */
    public static void sendMsg(String topic,String msg){
        //  Properties; //kafka的相关配置
        Properties properties = KakfaConfigUtil.getInstance().getKafkaProperties();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        kafkaProducer.send(new ProducerRecord<>(topic,msg));
        LOG.info("向kafka主题:"+topic+ "发送消息:" + msg);
        kafkaProducer.close();
    }
}
