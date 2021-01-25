package com.unicom.edgenode.kafka.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class JDBCUtils {

    public void insertAllByList(Connection connection,String tableName, List<Map<String, Object>> dataList, List<String> cols)
        throws Exception {
        PreparedStatement preparedStatement = null;
        int c = 0;

        try {
            String insertStr = buildInsertString(tableName, cols);
            preparedStatement = connection.prepareStatement(insertStr);
            connection.setAutoCommit(false);
            
            for (int x = 0; x < dataList.size(); x++) {
                Map<String, Object> data = dataList.get(x);
                for (int i = 0; i < cols.size(); i++) {
                    Object colValue = data.get(cols.get(i));
                    colValue = colValue == null ? "" : colValue;
                    try {
                        preparedStatement.setString(i + 1, String.valueOf(colValue));
                    } catch (Exception e) {
                        preparedStatement.setTimestamp(i + 1, null);
                    }
                }
                preparedStatement.addBatch();
                c += 1;
            }
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
            connection.commit();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("执行存入数据失败");
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new Exception("关闭连接失败");
            }
        }

    }

    public String buildInsertString(String tableName, List<String> columnKey) {
        StringBuffer columnSql = new StringBuffer("");
        StringBuffer unknownMarkSql = new StringBuffer("");
        StringBuffer sql = new StringBuffer("");
        for (int j = 0; j < columnKey.size(); j++) {
            String m = columnKey.get(j);
            columnSql.append(m);
            columnSql.append(",");

            unknownMarkSql.append("?");
            unknownMarkSql.append(",");
        }

        sql.append("INSERT INTO ");
        sql.append(tableName);
        sql.append(" (");
        sql.append(columnSql.substring(0, columnSql.length() - 1));
        sql.append(" )  VALUES (");
        sql.append(unknownMarkSql.substring(0, unknownMarkSql.length() - 1));
        sql.append(" )");
        return sql.toString();
    }

    public Map<String, Object> queryCacheData(Connection conn,String sql, String limit, List<Object> param) throws Exception {
        Map<String, Object> map = Maps.newHashMap();
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {

            // 如果limit含有逗号，则说明需要分页，根据pageNo和pageSize计算记录行数，并获取总记录数
            if (limit.indexOf(",") != -1) {
                String countSQL = String.format("select count(1) as c from (%s)", sql);
                PreparedStatement pstat = conn.prepareStatement(countSQL);
                ResultSet countRs = pstat.executeQuery();
                String count = "";
                while (countRs.next()) {
                    count = countRs.getString("c");
                }
                map.put("total", count);
            }

            String qrySQL =
                String.format("select * from (%s) x %s", sql, (limit.equals("-1") ? "" : (" limit " + limit)));

            preparedStatement = conn.prepareStatement(qrySQL);

            if (param != null && param.size() > 0) {
                for (int i = 1; i < param.size() + 1; i++) {
                    preparedStatement.setObject(i, param.get(i - 1));
                }
            }
            long startTime = System.currentTimeMillis();
            rs = preparedStatement.executeQuery();

            ResultSetMetaData md = rs.getMetaData();
            List<Map<String, Object>> ret = Lists.newArrayList();

            while (rs.next()) {
                Map<String, Object> rowMap = Maps.newHashMap();
                for (int i = 1; i < md.getColumnCount() + 1; i++) {
                    Object v = rs.getObject(i);
                    if (v instanceof java.math.BigDecimal) {
                        try {
                            rowMap.put(md.getColumnName(i).toLowerCase(), v.toString());
                        } catch (Exception e1) {
                            rowMap.put(md.getColumnName(i).toLowerCase(), v);
                            e1.printStackTrace();
                        }
                    } else {
                        rowMap.put(md.getColumnName(i).toLowerCase(), v);
                    }
                }
                ret.add(rowMap);
            }

            long endTime = System.currentTimeMillis();

            map.put("dataList", ret);
            return map;
        } catch (Exception e) {
            throw new Exception(">>>>>>queryCacheData 查询出错......................:" + e.getMessage());
        } finally {
            if (conn != null)
                conn.close();
            if (preparedStatement != null)
                preparedStatement.close();
            if (rs != null)
                rs.close();
        }
    }
}
