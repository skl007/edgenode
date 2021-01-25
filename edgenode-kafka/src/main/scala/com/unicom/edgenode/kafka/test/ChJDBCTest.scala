package com.unicom.edgenode.kafka.test

import com.unicom.edgenode.kafka.util.{ChDataSourceUtil, SqlProxy}

object ChJDBCTest {
  def main(args: Array[String]): Unit = {
    val sqlProxy = new SqlProxy()
    val chClient = ChDataSourceUtil.getConnection
    sqlProxy.executeUpdate(chClient, "insert into ods_base_station_china(lac, ci) values(?,?)", Array("1","2"));


  }
}
