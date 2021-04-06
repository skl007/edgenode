package com.unicom.edgenode.canal.Hanlder;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

import com.unicom.edgenode.canal.common.MyConstants;
import com.unicom.edgenode.canal.util.MyKafkaSender;

public class CanalHandler {
    String sql; //建表语句
    String databaseName;    //数据库名称
    String tableName;   //表名
    String topic; //topic
    CanalEntry.EventType eventType; //时间类型  insert update delete
    List<CanalEntry.RowData> rowDataList; //行级

    public CanalHandler(String sql,String databaseName,String tableName,String topic, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.sql=sql;
        this.databaseName=databaseName;
        this.tableName = tableName;
        this.topic = topic;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        if (eventType.equals(CanalEntry.EventType.INSERT)) {
            for (CanalEntry.RowData rowData : rowDataList) {
                //注意：因为消费Kafka时涉及到分区消费，为了保持数据一致性，建议topic分区只建一个。以防止小概率的上一个sql的某一条数据未消费，下一条sql的已经消费。
                sendKafka(tableName,eventType, rowData, topic);
            }
        }else if (eventType.equals(CanalEntry.EventType.UPDATE)) {
            for (CanalEntry.RowData rowData : rowDataList) {
                sendKafka(tableName,eventType, rowData, topic);
            }
        }else if (eventType.equals(CanalEntry.EventType.DELETE)) {
            for (CanalEntry.RowData rowData : rowDataList) {
                sendKafka(tableName,eventType, rowData, topic);
            }
        }else if (eventType.equals(CanalEntry.EventType.CREATE)) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("databaseName",databaseName);
            jsonObject.put("tableName",tableName);
            jsonObject.put("eventType",eventType);
            jsonObject.put("sql",sql);
            String rowJson = jsonObject.toJSONString();
            MyKafkaSender.send(topic, rowJson);
        }

    }

    /**
     * 发送kafka
     *
     * @param rowData
     * @param topic
     */
    private void sendKafka(String tableName,CanalEntry.EventType eventType, CanalEntry.RowData rowData, String topic) {
        List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
        List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("databaseName",databaseName);
        jsonObject.put("tableName",tableName);
        jsonObject.put("eventType",eventType);
        if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
            for (CanalEntry.Column column : columnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            String rowJson = jsonObject.toJSONString();
            // 发送数据到对应的topic中
            MyKafkaSender.send(topic, rowJson);
        }else if (eventType == CanalEntry.EventType.DELETE) {
            for (CanalEntry.Column column : beforeColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            String rowJson = jsonObject.toJSONString();
            // 发送数据到对应的topic中
            MyKafkaSender.send(topic, rowJson);
        }

    }


}