package com.li.ability.assessment

import java.io.File

import com.li.ability.assessment.udaf.PredictedScore
import com.li.ability.assessment.utils.SubjectAccumulator
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


object AbilityAssessment2 {

  def main(args: Array[String]): Unit = {


    //    System.setProperty("HADOOP_USER_NAME", "root")

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val conf = new SparkConf()
      .setAppName("AbilityAssessment2")
      .setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]], classOf[AnswerCard]))

    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config(conf)
      .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.implicits._

    //    val ztk_answer_card2 = sparkSession.sql("select userId,correct,question,answerTime,point,createTime,subject from ztk_answer_card")
    //
    //    ztk_answer_card2.show()
    //
    //    ztk_answer_card2.coalesce(300)
    //      .createOrReplaceTempView("spark_ztk_answer_card")

    sparkSession.udf.register("predictedScore", new PredictedScore)
    val predicted_score = sparkSession.sql("" +
      " select userId,predictedScore(correct,question,answerTime,point,createTime) predictedScore,subject " +
      " from ztk_answer_card " +
      " where createTime='20181201' " +
      " group by userId,subject")
    predicted_score.coalesce(300)

    predicted_score.cache()

    predicted_score.show()
    // 累加器
    /**
      * 全站
      */

    val xingCe = new SubjectAccumulator
    sc.register(xingCe, "xingCe")

    val gongJi = new SubjectAccumulator
    sc.register(gongJi, "gongJi")

    val zhiCe = new SubjectAccumulator
    sc.register(zhiCe, "zhiCe")

    val gongAn = new SubjectAccumulator
    sc.register(gongAn, "gongAn")

    /**
      * 周
      */


    val xingCeWeek = new SubjectAccumulator
    sc.register(xingCeWeek, "xingCeWeek")

    val gongJiWeek = new SubjectAccumulator
    sc.register(gongJiWeek, "gongJiWeek")

    val zhiCeWeek = new SubjectAccumulator
    sc.register(zhiCeWeek, "zhiCeWeek")

    val gongAnWeek = new SubjectAccumulator
    sc.register(gongAnWeek, "gongAnWeek")

    val predicted_score_rdd = predicted_score.rdd

    val ts_predicted_score_df = predicted_score_rdd.mapPartitions {
      ite =>
        var arr = new ArrayBuffer[TS_AbilityAssessment]()

        while (ite.hasNext) {
          val n = ite.next()
          val userId = n.get(0).asInstanceOf[Long].longValue()
          val predictedScore = n.get(1).asInstanceOf[Seq[String]].seq
          val subject = n.get(2).asInstanceOf[Int].intValue()

          if (subject == 1) {

            xingCe.add("1," + predictedScore(1) + "," + predictedScore(2) + "," + predictedScore(7))
          } else if (subject == 2) {

            gongJi.add("1," + predictedScore(1) + "," + predictedScore(2) + "," + predictedScore(7))
          } else if (subject == 3) {

            zhiCe.add("1," + predictedScore(1) + "," + predictedScore(2) + "," + predictedScore(7))
          } else if (subject == 100100175) {

            gongAn.add("1," + predictedScore(1) + "," + predictedScore(2) + "," + predictedScore(7))
          }

          arr += TS_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore.head, subject), //total_station_grade: Double,
            predictedScore.head, //total_station_predict_score
            predictedScore(1).toLong, //do_exercise_num
            predictedScore(2).toLong, //cumulative_time
            predictedScore(4).toLong, //do_exercise_day
            subject,
            predictedScore(7).toLong, //total_correct_num
            predictedScore(9).toLong
          )
        }
        arr.iterator
    }.toDF()
    ts_predicted_score_df.show()
    ts_predicted_score_df.createOrReplaceTempView("ts_predicted_score_df")
    val ts = sparkSession.sql("" +
      " select " +
      "userId," +
      "total_station_grade," +
      "total_station_predict_score," +
      "do_exercise_num," +
      "cumulative_time," +
      "do_exercise_day," +
      "subject," +
      "Row_Number() OVER(partition by subject order by total_station_grade desc) rank," +
      "Row_Number() OVER(partition by subject order by do_exercise_day desc) rank2," +
      "Row_Number() OVER(partition by subject order by cumulative_time desc) rank3," +
      "Row_Number() OVER(partition by subject order by do_exercise_num desc) rank4  " +
      "from ts_predicted_score_df")

    ts.show()

    val week_predicted_score_df = predicted_score_rdd.mapPartitions {
      ite =>
        var arr = new ArrayBuffer[Week_AbilityAssessment]()
        while (ite.hasNext) {
          val n = ite.next()
          val userId = n.get(0).asInstanceOf[Long].longValue()
          val predictedScore = n.get(1).asInstanceOf[Seq[String]].seq
          val subject = n.get(2).asInstanceOf[Int].intValue()

          if (subject == 1) {

            xingCeWeek.add("1," + predictedScore(5) + "," + predictedScore(6) + "," + predictedScore(8))
          } else if (subject == 2) {

            gongJiWeek.add("1," + predictedScore(5) + "," + predictedScore(6) + "," + predictedScore(8))
          } else if (subject == 3) {

            zhiCeWeek.add("1," + predictedScore(5) + "," + predictedScore(6) + "," + predictedScore(8))
          } else if (subject == 100100175) {

            gongAnWeek.add("1," + predictedScore(5) + "," + predictedScore(6) + "," + predictedScore(8))
          }


          arr += Week_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore(3), subject), //week_grade
            predictedScore(3), // week_predict_score
            subject,
            predictedScore(5).toLong,
            predictedScore(6).toLong,
            predictedScore(8).toLong,
            predictedScore(6).toLong * 1.0 / predictedScore(5).toLong,
            predictedScore(8).toLong * 1.0 / predictedScore(5).toLong,
            predictedScore(10).toLong
          )
        }
        arr.iterator
    }.toDF()
    week_predicted_score_df.createOrReplaceTempView("week_predicted_score_df")
    val week = sparkSession.sql("" +
      " select " +
      "userId," +
      "week_grade," +
      "week_predict_score," +
      "subject," +
      "Row_Number() OVER(partition by subject order by week_grade desc) rank, " +
      "week_do_exercise_num," +
      "week_cumulative_time," +
      "Row_Number() OVER(partition by subject order by week_do_exercise_num desc) rank2," +
      "Row_Number() OVER(partition by subject order by week_speek desc) rank3," +
      "Row_Number() OVER(partition by subject order by week_accuracy desc) rank4," +
      "week_speek," +
      "week_accuracy " +
      "from week_predicted_score_df  ")

    val weekTop10 = week.where("rank <= 10")

    val week_top10_hbaseConf = HBaseConfiguration.create()
    week_top10_hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    week_top10_hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    week_top10_hbaseConf.set("hbase.rootdir", "/hbase")
    week_top10_hbaseConf.set("hbase.client.retries.number", "300")
    week_top10_hbaseConf.set("hbase.rpc.timeout", "200000")
    week_top10_hbaseConf.set("hbase.client.operation.timeout", "30000")
    week_top10_hbaseConf.set("hbase.client.scanner.timeout.period", "10000")
    val week_top10_jobConf = new JobConf(week_top10_hbaseConf)
    week_top10_jobConf.setOutputFormat(classOf[TableOutputFormat])
    week_top10_jobConf.set(TableOutputFormat.OUTPUT_TABLE, "week_top10_ability_assessment")
    val week_top10_hbasePar = weekTop10.rdd.repartition(100).mapPartitions {
      ite: Iterator[Row] =>

        //          var lis: Seq[] = Seq()
        var buffer = new ArrayBuffer[(ImmutableBytesWritable, Put)]()

        while (ite.hasNext) {
          val t = ite.next()

          val userId = t.get(0).asInstanceOf[Long].longValue()


          val grade = t.get(2).asInstanceOf[String].toString
          val predictScore = t.get(1).asInstanceOf[Double].doubleValue()
          val subject = t.get(3).asInstanceOf[Int].intValue()
          val rank = t.get(4).asInstanceOf[Int].intValue()
          val exerciseNum = t.get(5).asInstanceOf[Long].longValue()
          val exerciseTime = t.get(6).asInstanceOf[Long].longValue()

          val put = new Put(Bytes.toBytes(rank + "-" + subject + "-" + TimeUtils.convertTimeStamp2DateStr(System.currentTimeMillis(), "yyyy-w"))) //行健的值
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userId"), Bytes.toBytes(userId.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("grade"), Bytes.toBytes(grade.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("predict_score"), Bytes.toBytes(predictScore.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseNum"), Bytes.toBytes(exerciseNum.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTime"), Bytes.toBytes(exerciseTime.toString))


          buffer += Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }

    week_top10_hbasePar.saveAsHadoopDataset(week_top10_jobConf)

    val _xingCe = sc.broadcast(xingCe.value)
    val _gongJi = sc.broadcast(gongJi.value)
    val _zhiCe = sc.broadcast(zhiCe.value)
    val _gongAn = sc.broadcast(gongAn.value)


    val _xingCeWeek = sc.broadcast(xingCeWeek.value)
    val _gongJiWeek = sc.broadcast(gongJiWeek.value)
    val _zhiCeWeek = sc.broadcast(zhiCeWeek.value)
    val _gongAnWeek = sc.broadcast(gongAnWeek.value)


    val weekHBaseConf = HBaseConfiguration.create()
    weekHBaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    weekHBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    weekHBaseConf.set("hbase.rootdir", "/hbase")
    weekHBaseConf.set("hbase.client.retries.number", "300")
    weekHBaseConf.set("hbase.rpc.timeout", "200000")
    weekHBaseConf.set("hbase.client.operation.timeout", "30000")
    weekHBaseConf.set("hbase.client.scanner.timeout.period", "100")
    val weekJobConf = new JobConf(weekHBaseConf)
    weekJobConf.setOutputFormat(classOf[TableOutputFormat])
    weekJobConf.set(TableOutputFormat.OUTPUT_TABLE, "week_ability_assessment")
    val weekData = week.rdd.repartition(100).mapPartitions {
      ite: Iterator[Row] =>

        //          var lis: Seq[] = Seq()
        var buffer = new ArrayBuffer[(ImmutableBytesWritable, Put)]()

        while (ite.hasNext) {
          val t = ite.next()

          val userId = t.get(0).asInstanceOf[Long].longValue()


          val grade = t.get(2).asInstanceOf[String].toString
          val predictScore = t.get(1).asInstanceOf[Double].doubleValue()
          val subject = t.get(3).asInstanceOf[Int].intValue()
          val rank = t.get(4).asInstanceOf[Int].intValue()
          val exerciseNum = t.get(5).asInstanceOf[Long].longValue()
          val exerciseTime = t.get(6).asInstanceOf[Long].longValue()
          val rank2 = t.get(7).asInstanceOf[Int].intValue()
          val rank3 = t.get(8).asInstanceOf[Int].intValue()
          val rank4 = t.get(9).asInstanceOf[Int].intValue()
          val week_speek = t.get(10).asInstanceOf[Double].doubleValue()
          val week_accuracy = t.get(11).asInstanceOf[Double].doubleValue()


          val put = new Put(Bytes.toBytes(userId + "-" + subject + "-" + TimeUtils.convertTimeStamp2DateStr(System.currentTimeMillis(), "yyyy-w"))) //行健的值
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("grade"), Bytes.toBytes(grade.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("predict_score"), Bytes.toBytes(predictScore.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseNum"), Bytes.toBytes(exerciseNum.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTime"), Bytes.toBytes(exerciseTime.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank2"), Bytes.toBytes(rank2.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank3"), Bytes.toBytes(rank3.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank4"), Bytes.toBytes(rank4.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_speek"), Bytes.toBytes(week_speek.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_accuracy"), Bytes.toBytes(week_accuracy.toString))

          var arr = Array("0", "0", "0", "0")
          if (subject == 1) {
            arr = _xingCeWeek.value.split(",")
          } else if (subject == 2) {
            arr = _gongJiWeek.value.split(",")
          } else if (subject == 3) {
            arr = _zhiCeWeek.value.split(",")
          } else if (subject == 100100175) {
            arr = _gongAnWeek.value.split(",")
          }

          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(arr(0)))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(arr(1)))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("timeTotal"), Bytes.toBytes(arr(2)))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(arr(3)))
          buffer += Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }

    weekData.saveAsHadoopDataset(weekJobConf)


    val HBaseConf = HBaseConfiguration.create()
    HBaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    HBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    HBaseConf.set("hbase.rootdir", "/hbase")
    HBaseConf.set("hbase.client.retries.number", "300")
    HBaseConf.set("hbase.rpc.timeout", "200000")
    HBaseConf.set("hbase.client.operation.timeout", "30000")
    HBaseConf.set("hbase.client.scanner.timeout.period", "100")

    val jobConf = new JobConf(HBaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "total_station_ability_assessment")
    val data = ts.rdd.repartition(100).mapPartitions {
      ite: Iterator[Row] =>

        var buffer = new ArrayBuffer[(ImmutableBytesWritable, Put)]()

        while (ite.hasNext) {
          val t = ite.next()

          val userId = t.get(0).asInstanceOf[Long].longValue()
          val total_station_grade = t.get(2).asInstanceOf[String].toString
          val total_station_predict_score = t.get(1).asInstanceOf[Double].doubleValue()
          val do_exercise_num = t.get(3).asInstanceOf[Long].longValue()
          val cumulative_time = t.get(4).asInstanceOf[Long].longValue()
          val do_exercise_day = t.get(5).asInstanceOf[Long].longValue()
          val subject = t.get(6).asInstanceOf[Int].intValue()
          val rank = t.get(7).asInstanceOf[Int].intValue()
          val rank2 = t.get(8).asInstanceOf[Int].intValue()
          val rank3 = t.get(9).asInstanceOf[Int].intValue()
          val rank4 = t.get(10).asInstanceOf[Int].intValue()


          val put = new Put(Bytes.toBytes(userId.toString + "-" + subject)) //行健的值
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("grade"), Bytes.toBytes(total_station_grade.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("predictScore"), Bytes.toBytes(total_station_predict_score.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseNum"), Bytes.toBytes(do_exercise_num.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTime"), Bytes.toBytes(cumulative_time.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseDay"), Bytes.toBytes(do_exercise_day.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank2"), Bytes.toBytes(rank2.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank3"), Bytes.toBytes(rank3.toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank4"), Bytes.toBytes(rank4.toString))


          var arr = Array("0", "0", "0", "0")
          if (subject == 1) {
            arr = _xingCe.value.split(",")
          } else if (subject == 2) {
            arr = _gongJi.value.split(",")
          } else if (subject == 3) {
            arr = _zhiCe.value.split(",")
          } else if (subject == 100100175) {
            arr = _gongAn.value.split(",")
          }
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(arr(0)))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(arr(1)))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(arr(2)))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(arr(3)))


          buffer += Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }
    data.saveAsHadoopDataset(jobConf)
    sparkSession.stop()
  }

}