package com.cn.wifi.es.client;

import com.cn.wifi.config.ConfigUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;


public class ESclientUtil {

    private static final Logger LOG = Logger.getLogger(ESclientUtil.class);

    private static final String ES_CONFIG_PATH = "es/es_cluster.properties";

    private volatile static TransportClient client;


    private static Properties properties;

    static {
        properties = ConfigUtil.getInstance().getProperties(ES_CONFIG_PATH);
    }

    private ESclientUtil(){}

    public static TransportClient getClient(){
        try {
            //解决netty冲突
            System.setProperty("es.set.netty.runtime.available.processors", "false");

            String host1 = properties.getProperty("es.cluster.nodes1").toString();
            String cluster_name = properties.getProperty("es.cluster.name").toString();

            Settings settings = Settings.builder()
                    .put("cluster.name", cluster_name).build();

            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host1), 9300));

        } catch (UnknownHostException e) {
            LOG.error(null,e);
        }
        return client;
    }
}
