package com.unicom.edgenode.canal.Hanlder;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

import com.unicom.edgenode.canal.common.MyConstants;
import com.unicom.edgenode.canal.util.MyKafkaSender;


public class CanalHandler {

    String tableName;   //表名
    CanalEntry.EventType eventType; //时间类型  insert update delete
    List<CanalEntry.RowData> rowDataList; //行级

    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        if (tableName.equals("student") && eventType.equals(CanalEntry.EventType.INSERT)) {
            for (CanalEntry.RowData rowData : rowDataList) {
                //注意：因为消费Kafka时涉及到分区消费，为了保持数据一致性，建议topic分区只建一个。以防止小概率的上一个sql的某一条数据未消费，下一条sql的已经消费。
                sendKafka(eventType, rowData, MyConstants.KAFKA_TOPIC_STUDENT);
            }
        }else if (tableName.equals("student") && eventType.equals(CanalEntry.EventType.UPDATE)) {
            for (CanalEntry.RowData rowData : rowDataList) {
                sendKafka(eventType, rowData, MyConstants.KAFKA_TOPIC_STUDENT);
            }
        }else if (tableName.equals("student") && eventType.equals(CanalEntry.EventType.DELETE)) {
            for (CanalEntry.RowData rowData : rowDataList) {
                sendKafka(eventType, rowData, MyConstants.KAFKA_TOPIC_STUDENT);
            }
        }
    }

    /**
     * 发送kafka
     *
     * @param rowData
     * @param topic
     */
    private void sendKafka(CanalEntry.EventType eventType, CanalEntry.RowData rowData, String topic) {
        List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
        JSONObject jsonObject = new JSONObject();
        if (eventType == CanalEntry.EventType.INSERT) {
            jsonObject.put("eventType",eventType);
            for (CanalEntry.Column column : columnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            String rowJson = jsonObject.toJSONString();
            // 发送数据到对应的topic中
            MyKafkaSender.send(topic, rowJson);
        }else if (eventType == CanalEntry.EventType.UPDATE) {
            jsonObject.put("eventType",eventType);
            for (CanalEntry.Column column : columnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            String rowJson = jsonObject.toJSONString();
            // 发送数据到对应的topic中
            MyKafkaSender.send(topic, rowJson);
        }else if (eventType == CanalEntry.EventType.DELETE) {
            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
            jsonObject.put("eventType",eventType);
            for (CanalEntry.Column column : beforeColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            String rowJson = jsonObject.toJSONString();
            // 发送数据到对应的topic中
            MyKafkaSender.send(topic, rowJson);
        }

    }


}