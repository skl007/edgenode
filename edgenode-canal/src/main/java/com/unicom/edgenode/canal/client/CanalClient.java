package com.unicom.edgenode.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.unicom.edgenode.canal.Hanlder.CanalHandler;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {


    public static void main(String[] args) {
        //建立连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while(true){
            canalConnector.connect();  //尝试连接
            canalConnector.subscribe("edgenode.*");  //过滤数据
            Message message = canalConnector.get(100); //抓取数据

            if(message.getEntries().size()==0){
//                System.out.println("没有数据,sleep几秒...");
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //只有行变化才处理
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        CanalEntry.RowChange rowChange = null;

                        try {
                            // 把entry中的数据进行反序列化
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        String sql = rowChange.getSql();
                        CanalEntry.EventType eventType = rowChange.getEventType();//insert update delete
                        String tableName = entry.getHeader().getTableName();
                        String databaseName = entry.getHeader().getSchemaName();

                        CanalHandler canalHanlder = new CanalHandler(sql,databaseName,tableName, eventType, rowDatasList);

                        canalHanlder.handle();
                    }



                }


            }


        }

    }
}