package com.eric.lab.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

public class JsonUtils {
    public static String parseMap2Json(Map<String,Object> map){
        return JSONObject.toJSONString(map);
    }

    public static Map<String, Object> parseJSON2Map(String jsonStr){
        Map<String, Object> map = JSONObject.parseObject(jsonStr);
        return map;
    }

    public static void main(String[] args){
    }
}
