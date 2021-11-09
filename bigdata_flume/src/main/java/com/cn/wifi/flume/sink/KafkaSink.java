package com.cn.wifi.flume.sink;

import com.cn.wifi.kafka.producer.StringProducer;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;


public class KafkaSink extends AbstractSink implements Configurable {
    private String kafkaTopic;
    private static final Logger LOG = Logger.getLogger(KafkaSink.class);


    @Override
    public void configure(Context context) {
        kafkaTopic = context.getString("kafkaTopic");

    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null; //状态
        // Start transaction
        Channel ch = getChannel(); //获取channel
        //启动事务
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take(); //从channel中获取数据

            if(event == null){
                txn.rollback();
                return Status.BACKOFF;
            }
            String line = new String(event.getBody());
            if(line!=null){
                StringProducer.sendMsg(kafkaTopic,line);
                LOG.error("==========数据发送成功==============" + line);
            }
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }


    @Override
    public synchronized void start() {
    }

    @Override
    public synchronized void stop() {
    }
}
