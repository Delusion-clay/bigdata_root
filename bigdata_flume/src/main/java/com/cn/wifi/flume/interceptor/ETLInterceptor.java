package com.cn.wifi.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.cn.wifi.flume.constant.ConstantFields;
import com.cn.wifi.flume.service.DataCheck;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ETLInterceptor implements Interceptor {
    private static final Logger LOG = Logger.getLogger(ETLInterceptor.class);
    @Override
    public void initialize() {

    }
    /**
     * 拦截方法
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //实现数据处理业务逻辑的地方
        Map<String,String> header = event.getHeaders();
        String fileName = header.get(ConstantFields.FILE_NAME);
        String absolute_filename = header.get(ConstantFields.ABSOLUTE_FILENAME);

        String line = new String(event.getBody(), Charsets.UTF_8);
        //TODO 1.ETL
        //TODO 1. 数据字段转换
        //TODO 2. 数据加工
        //TODO 3. 数据校验
        //TODO 4. 数据修复
        Map map = DataCheck.txtParseAndValidation(fileName,absolute_filename,line);
        String json = JSON.toJSONString(map);
        LOG.error("拦截器执行=》"+json );
        Event eventNew = new SimpleEvent();
        eventNew.setBody(json.getBytes());
        return eventNew;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> listEvents = new ArrayList<>();
        for (Event event : list) {
            Event intercept = intercept(event);
            if(intercept!=null){
                listEvents.add(intercept);
            }
        }
        return listEvents;
    }

    public static class Builder implements Interceptor.Builder{
        public Builder() {
            super();
        }

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

    @Override
    public void close() {

    }
}
