package com.lzl

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


import java.sql
import java.sql.{Connection, DriverManager}
import java.util.Properties

object FlumeKafkaFlinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置时间戳
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //设置并行度为1
    env.setParallelism(1)
    //确认数据来源
        val prop = new Properties()
        prop.setProperty("bootstrap.servers","192.168.200.130:9092,192.168.200.131:9092,192.168.200.132:9092")
        prop.setProperty("group.id","test")
        val inputStream = env.addSource(new FlinkKafkaConsumer[String]("mytest",new SimpleStringSchema(),prop))

    //从端口接受数据进行测试
    //val inputStream = env.socketTextStream("192.168.200.130",20000)

    //对数据进行处理
    val dataStream = inputStream.filter(x=>{
      val arr = x.split(",")
      val state = arr(2) //获取该行数据的订单状态
      if(state.equals("1003") || state.equals("1005") || state.equals("1006")){
        false
      }
      else {
        true
      }
    })

    //对过滤活动数据进行计算
    val ordersStream = dataStream.map(x=>{
      ("total_orders",1)
    }).keyBy(_._1).timeWindow(Time.minutes(1)).sum(1)

//    ordersStream.print()
//    println("----------------------------------------------")

    //将数据存到redis中
        val conf = new FlinkJedisPoolConfig.Builder().setHost("172.20.38.73").setPort(6379).build()
        ordersStream.addSink(new RedisSink[(String, Int)](conf,new RedisMapper[(String, Int)] {
          override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

          override def getKeyFromData(t: (String, Int)): String = t._1

          override def getValueFromData(t: (String, Int)): String = t._2.toString
        }))

    //每分钟内各个用户的有效订单总额
    val userTotalStream = dataStream.map(x=>{
      //分割数据
      val arr = x.split(",")
      val user = arr(1) //用户名
      val price = arr(3).toDouble //该用户的订单金额
      (user,price)
    }).keyBy(_._1).timeWindow(Time.minutes(1)).sum(1)

//    userTotalStream.print()
//    println("---------------------------------------------")

    //将userTotalStream结果存入到MySQL
    userTotalStream.addSink(new MysqlSink())

    env.execute(jobName = "test")
  }

  //自己写一个存入MySQL的自定义sink使用
  class MysqlSink extends RichSinkFunction[(String,Double)]{
    //打开数据库
    var conn:Connection = _
    var pstmt:sql.PreparedStatement = _
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://172.20.38.73:3306/flink_test?characterEncoding=utf-8","root","5131420lr")
      pstmt = conn.prepareStatement("insert into tb_user_orders values (?,?)")
    }
    //要干啥
    override def invoke(value: (String, Double), context: SinkFunction.Context): Unit = {
      pstmt.setString(1,value._1)
      pstmt.setDouble(2,value._2)
    }
    //关闭数据库
    override def close(): Unit = {
      pstmt.close()
      conn.close()
    }
  }
}
