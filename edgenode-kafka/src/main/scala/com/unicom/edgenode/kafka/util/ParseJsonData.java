package com.unicom.edgenode.kafka.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


public class ParseJsonData {

    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }

    public  static JSONArray getJsonArr(String stringArr){
        try {
            JSONArray jsonArr = JSON.parseArray(stringArr);
            return jsonArr;
        } catch (Exception e) {
            return null;
        }
    }
}
