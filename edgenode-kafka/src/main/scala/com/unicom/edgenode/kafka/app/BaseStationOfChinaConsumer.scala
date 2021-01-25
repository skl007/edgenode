package com.unicom.edgenode.kafka.app

import java.io.{BufferedWriter, FileWriter, IOException}
import java.sql.ResultSet
import java.util
import java.util.{List, Map, Properties}

import com.google.common.collect.{Lists, Maps}
import com.unicom.edgenode.kafka.util.{ChDataSourceUtil, DataSourceUtil, JDBCUtils, MyKafkaUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable

object BaseStationOfChinaConsumer {
  //初始化消费者组
  private val groupid = "base_station_china_consumer"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("BaseStationOfChinaConsumer")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //将要消费的kafka的topic
    val topic = "base_station_china"
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection

    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=? and topic=?", Array(groupid, topic), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      MyKafkaUtil.getKafkaStream(groupid, topic, ssc)
    } else {
      MyKafkaUtil.getKafkaStream(groupid, topic, ssc, offsetMap)
    }


    /*val value = stream.map(item => item.value())
    value.foreachRDD(rdd=>println(rdd.collect().mkString("\n")))*/
    //解析json数据
    //过滤不正常数据 获取数据

    val dsStream = stream.filter(item => item.value().split("\\|").length == 16) mapPartitions (partition => partition.map(item => {
      val line = item.value()
      val arr = line.split("\\|")

      val lac = arr(0)
      val ci = arr(1)
      val lat = arr(2)
      val lon = arr(3)
      val rev_sta = arr(4)
      val area_id = arr(5)
      val prov_id = arr(6)
      val net_type = arr(7)
      val enodeb_id = arr(8)
      val cell_id = arr(9)
      val cell_type = arr(10)
      val prov_desc = arr(11)
      val area_desc = arr(12)
      val district_desc = arr(13)
      val district_id = arr(14)

     //clickhouse 按照时间分区，产生太多小文件了，所以把时间定死
     val update_time = "2020-12-21 16:56:00"
      (lac, ci, lat, lon, rev_sta, area_id, prov_id, net_type, enodeb_id, cell_id, cell_type, prov_desc, area_desc, district_desc, district_id, update_time)
    }))

    //    dsStream.foreachRDD(rdd=>println("打印ebuyDStream: "+rdd.collect().mkString("\n")))
    dsStream.cache()

    //将数据存入clickhouse
    dsStream.foreachRDD(rdd => {
      rdd.foreachPartition(partitions => {
        val jdbcUtils = new JDBCUtils
        val chClient = ChDataSourceUtil.getConnection
        val datas = Lists.newArrayList[Map[String, Object]]
        val cols = Lists.newArrayList("lac", "ci", "lat", "lon", "rev_sta", "area_id", "prov_id", "net_type", "enodeb_id", "cell_id", "cell_type", "prov_desc", "area_desc", "district_desc", "district_id", "update_time")

        partitions.foreach(model => {
            val m = Maps.newHashMap[String, Object]
            m.put("lac",model._1)
            m.put("ci",model._2)
            m.put("lat",model._3)
            m.put("lon",model._4)
            m.put("rev_sta",model._5)
            m.put("area_id",model._6)
            m.put("prov_id",model._7)
            m.put("net_type",model._8)
            m.put("enodeb_id",model._9)
            m.put("cell_id",model._10)
            m.put("cell_type",model._11)
            m.put("prov_desc",model._12)
            m.put("area_desc",model._13)
            m.put("district_desc",model._14)
            m.put("district_id",model._15)
            m.put("update_time",model._16)
            datas.add(m)
            //向ebuy_plansalescity表中插入或更新数据
//            chsqlProxy.executeUpdate(chClient, "insert into ods_base_station_china(lac, ci, lat, lon, rev_sta, area_id, prov_id, net_type, enodeb_id, cell_id, cell_type, prov_desc, area_desc, district_desc, district_id, update_time)values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
//              Array(lac, ci, lat, lon, rev_sta, area_id, prov_id, net_type, enodeb_id, cell_id, cell_type, prov_desc, area_desc, district_desc, district_id, update_time))
          })

        jdbcUtils.insertAllByList(chClient,"ods_base_station_china", datas, cols)
        datas.clear()
      })

    })


    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val mysqlProxy = new SqlProxy()
      val mysqlclient = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          mysqlProxy.executeUpdate(mysqlclient, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        mysqlProxy.shutdown(mysqlclient)
      }
    })

    println("启动流程")
    ssc.start()
    ssc.awaitTermination()

  }
}
