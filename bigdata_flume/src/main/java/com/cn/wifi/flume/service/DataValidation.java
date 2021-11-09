package com.cn.wifi.flume.service;

import com.cn.wifi.flume.constant.ErrorMapFields;
import com.cn.wifi.flume.constant.MapFields;
import com.cn.wifi.regex.Validation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class DataValidation {
    private static final Logger LOG = Logger.getLogger(DataValidation.class);

    public static Map<String,Object> dataValidation(Map<String,String> map) {
        LOG.error("=======5======" + map);
        if (map == null) {
            return null;
        }
        LOG.error("=======6======" + map);
        //检测校验
        //TODO 大小写统一
        //TODO 时间类型统一
        //TODO 数据字段类型统一
        // 数据修复  针对可以补全或者可以修复的数据进行修复
        //TODO 空处理
        //TODO 业务数据清洗

        //存放异常信息的集合
        Map<String,Object> errorMap = new HashMap<>();
        //验证手机号码 是不是手机号码
        DataValidation.sjhmValidation(map,errorMap);
        LOG.error("=======7======" + map);
        //验证MAC ，是不是MAC地址，统一大小写
        DataValidation.macValidation(map,errorMap);
        //经纬度验证
        LOG.error("=======8======" + map);

        return errorMap;
    }
    /**
     * 手机号码验证
     * @param map
     * @param errorMap
     */
    public static void sjhmValidation(Map<String,String> map,Map<String,Object> errorMap){
        LOG.error("=======6-0======" + map);
        if(map.containsKey(MapFields.PHONE)){
            LOG.error("=======6-1======" + map);
            String sjhm = map.get(MapFields.PHONE);
            LOG.error("=======6-2======" + sjhm);
            //正则验证
            boolean isMobile = false;
            try {
                LOG.error("=======6-2-2======" + map);
                isMobile = Validation.isMobile(sjhm);
                LOG.error("=======6-3======" + map);
            } catch (Exception e) {
                LOG.error("=======6-4======" + map);
                LOG.error(null,e);
                e.printStackTrace();
            }
            LOG.error("=======6-5======" + map);
            if(!isMobile){
                LOG.error("=======6-6======" + map);
                LOG.error("========手机号码格式不对:"+sjhm);
                errorMap.put(ErrorMapFields.SJHM,sjhm);
                errorMap.put(ErrorMapFields.SJHM_ERROR,ErrorMapFields.SJHM_ERRORCODE);
                LOG.error("=======6-7======" + map);
            }
        }
    }


    public static void macValidation(Map<String,String> map,Map<String,Object> errorMap){
        if(map.containsKey(MapFields.PHONE_MAC)){
            String mac = map.get(MapFields.PHONE_MAC);
            if(StringUtils.isNotBlank(mac)){
                boolean isMac = Validation.isMac(mac);
                if(!isMac){
                    errorMap.put(ErrorMapFields.MAC,mac);
                    errorMap.put(ErrorMapFields.MAC_ERROR,ErrorMapFields.MAC_ERRORCODE);
                }
            }else{
                LOG.error("MAC为空");
                errorMap.put(ErrorMapFields.MAC,mac);
                errorMap.put(ErrorMapFields.MAC_ERROR,ErrorMapFields.MAC_ERRORCODE);
            }
        }
    }

}
