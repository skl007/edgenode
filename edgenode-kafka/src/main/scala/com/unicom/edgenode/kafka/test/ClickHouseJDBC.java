package com.unicom.edgenode.kafka.test;


import com.unicom.edgenode.kafka.util.ChDataSourceUtil;
import com.unicom.edgenode.kafka.util.SqlProxy;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ClickHouseJDBC {
    public static void main(String[] args) {
        /*String sqlDB = "show databases";//查询数据库
        String sqlTab = "show tables";//查看表
        String sqlCount = "select count(*) count from ods_base_station_china";//查询ontime数据量
        exeSql(sqlDB);
        exeSql(sqlTab);
        exeSql(sqlCount);*/
        SqlProxy sqlProxy = new SqlProxy();

        try {
            Connection connection = ChDataSourceUtil.getConnection();

            String lac = "11";
            String ci = "23";
//            sqlProxy.executeUpdate(connection, "insert into ods_base_station_china(lac, ci) values(?,?)", ;
//            System.out.println(connection);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void exeSql(String sql){
        String address = "jdbc:clickhouse://172.31.10.242:8123/default";
        Connection connection = null;
        Statement statement = null;
        ResultSet results = null;
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            connection = DriverManager.getConnection(address);
            statement = connection.createStatement();
            long begin = System.currentTimeMillis();
            results = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            System.out.println("执行（"+sql+"）耗时："+(end-begin)+"ms");
            ResultSetMetaData rsmd = results.getMetaData();
            List<Map> list = new ArrayList();
            while(results.next()){
                Map map = new HashMap();
                for(int i = 1;i<=rsmd.getColumnCount();i++){
                    map.put(rsmd.getColumnName(i),results.getString(rsmd.getColumnName(i)));
                }
                list.add(map);
            }
            for(Map map : list){
                System.err.println(map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {//关闭连接
            try {
                if(results!=null){
                    results.close();
                }
                if(statement!=null){
                    statement.close();
                }
                if(connection!=null){
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
