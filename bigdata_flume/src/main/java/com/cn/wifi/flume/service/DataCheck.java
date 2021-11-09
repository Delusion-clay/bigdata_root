package com.cn.wifi.flume.service;

import com.cn.wifi.config.ConfigUtil;
import com.cn.wifi.flume.constant.ConstantFields;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;


public class DataCheck {
    private static final Logger LOG = Logger.getLogger(DataCheck.class);

    //数据结构字典
    static Properties properties;

    static {
        String tablePath = "common/table.properties";
        LOG.error("获取properties"+"=================="+tablePath);
        properties = ConfigUtil.getInstance().getProperties(tablePath);
        LOG.error("获取配置成功");
    }

    public static Map txtParseAndValidation(String fileName, String absolute_filename, String line) {
        //TODO 1.ETL
        //TODO 1. 数据字段转换
        Map data = new HashMap<String, String>();//存放解析之后的数据
        //异常数据，错误数据存储
        Map errorMap = new HashMap<String,String>();
        //数据类型
        LOG.error("==================数据检查开始==================");
        String table = fileName.split("_")[0].toLowerCase();
        String[] fields = properties.getProperty(table).split(",");
        String[] values = line.split("\t");
        if (fields.length == values.length) {
            //TODO 字段和值的映射
            for (int i = 0; i < fields.length; i++) {
                data.put(fields[i], values[i]);
            }
            //为了满足需求，需要进一步处理，添加文件路径，文件名
            //为了回找数据源
            //TODO 2. 数据加工
            /**
             *  数据加工是为了满足后续的业务需求而对数据进行相关处理
             */
            data.put(ConstantFields.FILE_NAME,fileName);
            data.put("table",table);
            data.put(ConstantFields.ABSOLUTE_FILENAME,absolute_filename);
            data.put(ConstantFields.ID, UUID.randomUUID().toString().replace("-",""));
            data.put(ConstantFields.RKSJ,System.currentTimeMillis()/1000+"");
        } else {
            errorMap.put("leng_error","字段数和值的个数长度不匹配");
        }
        //TODO 3. 数据校验
        //判断数据有没有异常，数据清洗  （-180，180） 不能直接丢弃
        //异常数据库   为了检测数据质量。
        if(data!=null && data.size()>0){
            LOG.error("=======4======" + data);
            errorMap = DataValidation.dataValidation(data);
            LOG.error("=======9======" + data);
        }
        //判断数据是否有问题
        if(errorMap.size()>0){
            LOG.error("errorMap  5 :" + errorMap);
            //如果数据有问题，不写入kafka,不返回数据，然后错误数据写入ES
            //TODO 将错误数据写入ES
            //把数据置为空
            data = null;
        }


        return data;
    }

    public static void main(String[] args) {
        System.out.println(properties);
    }

}
