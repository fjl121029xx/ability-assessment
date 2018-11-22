package com.li.ability.assessment

import java.io.File
import java.text.SimpleDateFormat

import com.li.ability.assessment.udaf.PredictedScore
import com.li.ability.assessment.udaf.PredictedScore.getTSPredictScore2Map
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark.config.ReadConfig
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ArrayBuffer, Map}


object AbilityAssessment2 {

  def main(args: Array[String]): Unit = {


    System.setProperty("HADOOP_USER_NAME", "root")

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val conf = new SparkConf()
      .setAppName("AbilityAssessment2")
//      .setMaster("local[3]")
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
    val predicted_score = sparkSession.sql("select userId,predictedScore(correct,question,answerTime,point,createTime) predictedScore,subject from ztk_answer_card  where createTime = '2018-11-20' group by userId,subject")
    predicted_score.coalesce(300)

    predicted_score.show()
    println(predicted_score.count)
    // 累加器
    /**
      * 全站
      */
    // 统计科目下的用户数量
    val ac_xingce_all_user_count = sc.longAccumulator("ac_xingce_all_user_count")
    val ac_gongji_all_user_count = sc.longAccumulator("ac_gongji_all_user_count")
    val ac_zhice_all_user_count = sc.longAccumulator("ac_zhice_all_user_count")
    // 统计科目下的做题数量
    val ac_xingce_all_ques_count = sc.longAccumulator("ac_xingce_all_ques_count")
    val ac_gongji_all_ques_count = sc.longAccumulator("ac_gongji_all_ques_count")
    val ac_zhice_all_ques_count = sc.longAccumulator("ac_zhice_all_ques_count")
    // 统计科目下的做题时长
    val ac_xingce_all_time_total = sc.longAccumulator("ac_xingce_all_time_total")
    val ac_gongji_all_time_total = sc.longAccumulator("ac_gongji_all_time_total")
    val ac_zhice_all_time_total = sc.longAccumulator("ac_zhice_all_time_total")
    // 统计科目下的正确数量
    val ac_xingce_all_correct_num = sc.longAccumulator("ac_xingce_all_correct_num")
    val ac_gongji_all_correct_num = sc.longAccumulator("ac_gongji_all_correct_num")
    val ac_zhice_all_correct_num = sc.longAccumulator("ac_zhice_all_correct_num")
    /**
      * 周
      */
    // 周:统计科目下的用户数量
    val ac_xingce_week_user_count = sc.longAccumulator("ac_xingce_week_user_count")
    val ac_gongji_week_user_count = sc.longAccumulator("ac_gongji_week_user_count")
    val ac_zhice_week_user_count = sc.longAccumulator("ac_zhice_week_user_count")

    // 周:统计科目下的做题数量
    val ac_xingce_week_ques_count = sc.longAccumulator("ac_xingce_week_ques_count")
    val ac_gongji_week_ques_count = sc.longAccumulator("ac_gongji_week_ques_count")
    val ac_zhice_week_ques_count = sc.longAccumulator("ac_zhice_week_ques_count")
    // 周:统计科目下的做题时长
    val ac_xingce_week_time_total = sc.longAccumulator("ac_xingce_week_time_total")
    val ac_gongji_week_time_total = sc.longAccumulator("ac_gongji_week_time_total")
    val ac_zhice_week_time_total = sc.longAccumulator("ac_zhice_week_time_total")
    // 周:统计科目下的正确数量
    val ac_xingce_week_correct_num = sc.longAccumulator("ac_xingce_week_correct_num")
    val ac_gongji_week_correct_num = sc.longAccumulator("ac_gongji_week_correct_num")
    val ac_zhice_week_correct_num = sc.longAccumulator("ac_zhice_week_correct_num")

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
            ac_xingce_all_user_count.add(1)
            ac_xingce_all_ques_count.add(predictedScore(1).toLong)
            ac_xingce_all_time_total.add(predictedScore(2).toLong)
            ac_xingce_all_correct_num.add(predictedScore(7).toLong)
          } else if (subject == 2) {
            ac_gongji_all_user_count.add(1)
            ac_gongji_all_ques_count.add(predictedScore(1).toLong)
            ac_gongji_all_time_total.add(predictedScore(2).toLong)
            ac_gongji_all_correct_num.add(predictedScore(7).toLong)
          } else if (subject == 3) {
            ac_zhice_all_user_count.add(1)
            ac_zhice_all_ques_count.add(predictedScore(1).toLong)
            ac_zhice_all_time_total.add(predictedScore(2).toLong)
            ac_zhice_all_correct_num.add(predictedScore(7).toLong)
          }

          arr += TS_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore(0), subject), //total_station_grade: Double,
            predictedScore(0), //total_station_predict_score
            predictedScore(1).toLong, //do_exercise_num
            predictedScore(2).toLong, //cumulative_time
            predictedScore(4).toLong, //do_exercise_day
            subject,
            predictedScore(7).toLong //total_correct_num
          )
        }
        arr.iterator
    }.toDF()
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

    //    ts.show(5000)
    println(ts.count())

    val tsTop10 = ts.where("subject == 1").where("rank4 <= 10")
    tsTop10.show(10)


    val week_predicted_score_df = predicted_score_rdd.mapPartitions {
      ite =>
        var arr = new ArrayBuffer[Week_AbilityAssessment]()
        while (ite.hasNext) {
          val n = ite.next()
          val userId = n.get(0).asInstanceOf[Long].longValue()
          val predictedScore = n.get(1).asInstanceOf[Seq[String]].seq
          val subject = n.get(2).asInstanceOf[Int].intValue()

          if (subject == 1) {
            ac_xingce_week_user_count.add(1)
            ac_xingce_week_ques_count.add(predictedScore(1).toLong)
            ac_xingce_week_time_total.add(predictedScore(2).toLong)
            ac_xingce_week_correct_num.add(predictedScore(8).toLong)
          } else if (subject == 2) {
            ac_gongji_week_user_count.add(1)
            ac_gongji_week_ques_count.add(predictedScore(1).toLong)
            ac_gongji_week_time_total.add(predictedScore(2).toLong)
            ac_gongji_week_correct_num.add(predictedScore(8).toLong)
          } else if (subject == 3) {
            ac_zhice_week_user_count.add(1)
            ac_zhice_week_ques_count.add(predictedScore(1).toLong)
            ac_zhice_week_time_total.add(predictedScore(2).toLong)
            ac_zhice_week_correct_num.add(predictedScore(8).toLong)
          }
          /*  userId: Long,
            week_grade: Double,
            week_predict_score: String,
            subject: Int,
            week_do_exercise_num: Long,
            week_cumulative_time: Long,
            week_correct_num: Long,
            week_speek: Double,
            week_accuracy: Double*/

          arr += Week_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore(3), subject), //week_grade
            predictedScore(3), // week_predict_score
            subject,
            predictedScore(5).toLong,
            predictedScore(6).toLong,
            predictedScore(8).toLong,
            predictedScore(6).toLong * 1.0 / predictedScore(5).toLong,
            predictedScore(8).toLong * 1.0 / predictedScore(5).toLong
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
    //    week.show(5000)
    print(week.count())

    val weekTop10 = week.where("rank <= 10")
    //    weekTop10.show(10)


    val week_top10_hbaseConf = HBaseConfiguration.create()
    week_top10_hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    week_top10_hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    week_top10_hbaseConf.set("hbase.rootdir", "/hbase")
    week_top10_hbaseConf.set("hbase.client.retries.number", "3")
    week_top10_hbaseConf.set("hbase.rpc.timeout", "200000")
    week_top10_hbaseConf.set("hbase.client.operation.timeout", "30")
    week_top10_hbaseConf.set("hbase.client.scanner.timeout.period", "100")
    val week_top10_jobConf = new JobConf(week_top10_hbaseConf)
    week_top10_jobConf.setOutputFormat(classOf[TableOutputFormat])
    week_top10_jobConf.set(TableOutputFormat.OUTPUT_TABLE, "week_top10_ability_assessment")
    val week_top10_hbasePar = weekTop10.rdd.coalesce(1).mapPartitions {
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
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userId"), Bytes.toBytes(userId.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("grade"), Bytes.toBytes(grade.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("predict_score"), Bytes.toBytes(predictScore.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseNum"), Bytes.toBytes(exerciseNum.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTime"), Bytes.toBytes(exerciseTime.toString))


          buffer += new Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }

    week_top10_hbasePar.saveAsHadoopDataset(week_top10_jobConf)


    val xingce_week_user_count = sc.broadcast(ac_xingce_week_user_count.value.toString)
    val xingce_week_ques_count = sc.broadcast(ac_xingce_week_ques_count.value.toString)
    val xingce_week_time_total = sc.broadcast(ac_xingce_week_time_total.value.toString)
    val xingce_week_correct_num = sc.broadcast(ac_xingce_week_correct_num.value.toString)

    val gongji_week_user_count = sc.broadcast(ac_gongji_week_user_count.value.toString)
    val gongji_week_ques_count = sc.broadcast(ac_gongji_week_ques_count.value.toString)
    val gongji_week_time_total = sc.broadcast(ac_gongji_week_time_total.value.toString)
    val gongji_week_correct_num = sc.broadcast(ac_gongji_week_correct_num.value.toString)

    val zhice_week_user_count = sc.broadcast(ac_zhice_week_user_count.value.toString)
    val zhice_week_ques_count = sc.broadcast(ac_zhice_week_ques_count.value.toString)
    val zhice_week_time_total = sc.broadcast(ac_zhice_week_time_total.value.toString)
    val zhice_week_correct_num = sc.broadcast(ac_zhice_week_correct_num.value.toString)


    val week_hbaseConf = HBaseConfiguration.create()
    week_hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    week_hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    week_hbaseConf.set("hbase.rootdir", "/hbase")
    week_hbaseConf.set("hbase.client.retries.number", "3")
    week_hbaseConf.set("hbase.rpc.timeout", "200000")
    week_hbaseConf.set("hbase.client.operation.timeout", "30")
    week_hbaseConf.set("hbase.client.scanner.timeout.period", "100")
    val week_jobConf = new JobConf(week_hbaseConf)
    week_jobConf.setOutputFormat(classOf[TableOutputFormat])
    week_jobConf.set(TableOutputFormat.OUTPUT_TABLE, "week_ability_assessment")
    val week_hbasePar = week.rdd.coalesce(1).mapPartitions {
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
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("grade"), Bytes.toBytes(grade.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("predict_score"), Bytes.toBytes(predictScore.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseNum"), Bytes.toBytes(exerciseNum.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTime"), Bytes.toBytes(exerciseTime.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank2"), Bytes.toBytes(rank2.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank3"), Bytes.toBytes(rank3.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank4"), Bytes.toBytes(rank4.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_speek"), Bytes.toBytes(week_speek.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_accuracy"), Bytes.toBytes(week_accuracy.toString))


          if (subject == 1) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(xingce_week_user_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(xingce_week_ques_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("timeTotal"), Bytes.toBytes(xingce_week_time_total.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(xingce_week_correct_num.value))
          } else if (subject == 2) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(gongji_week_user_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(gongji_week_ques_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("timeTotal"), Bytes.toBytes(gongji_week_time_total.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(gongji_week_correct_num.value))
          } else if (subject == 3) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(zhice_week_user_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(zhice_week_ques_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("timeTotal"), Bytes.toBytes(zhice_week_time_total.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(zhice_week_correct_num.value))
          }

          buffer += new Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }

    week_hbasePar.saveAsHadoopDataset(week_jobConf)


    val xingce_all_user_count = sc.broadcast(ac_xingce_all_user_count.value.toString)
    val xingce_all_ques_count = sc.broadcast(ac_xingce_all_ques_count.value.toString)
    val xingce_all_time_total = sc.broadcast(ac_xingce_all_time_total.value.toString)
    val xingce_all_correct_num = sc.broadcast(ac_xingce_all_correct_num.value.toString)

    val gongji_all_user_count = sc.broadcast(ac_gongji_all_user_count.value.toString)
    val gongji_all_ques_count = sc.broadcast(ac_gongji_all_ques_count.value.toString)
    val gongji_all_time_total = sc.broadcast(ac_gongji_all_time_total.value.toString)
    val gongji_all_correct_num = sc.broadcast(ac_gongji_all_correct_num.value.toString)

    val zhice_all_user_count = sc.broadcast(ac_zhice_all_user_count.value.toString)
    val zhice_all_ques_count = sc.broadcast(ac_zhice_all_ques_count.value.toString)
    val zhice_all_time_total = sc.broadcast(ac_zhice_all_time_total.value.toString)
    val zhice_all_correct_num = sc.broadcast(ac_zhice_all_correct_num.value.toString)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.rootdir", "/hbase")
    hbaseConf.set("hbase.client.retries.number", "3")
    hbaseConf.set("hbase.rpc.timeout", "200000")
    hbaseConf.set("hbase.client.operation.timeout", "30")
    hbaseConf.set("hbase.client.scanner.timeout.period", "100")

    val total_station_jobConf = new JobConf(hbaseConf)
    total_station_jobConf.setOutputFormat(classOf[TableOutputFormat])
    total_station_jobConf.set(TableOutputFormat.OUTPUT_TABLE, "total_station_ability_assessment")
    val ts_hbasePar = ts.rdd.coalesce(1).mapPartitions {
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
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("grade"), Bytes.toBytes(total_station_grade.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("predictScore"), Bytes.toBytes(total_station_predict_score.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseNum"), Bytes.toBytes(do_exercise_num.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTime"), Bytes.toBytes(cumulative_time.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseDay"), Bytes.toBytes(do_exercise_day.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank2"), Bytes.toBytes(rank2.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank3"), Bytes.toBytes(rank3.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank4"), Bytes.toBytes(rank3.toString))


          if (subject == 1) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(xingce_all_user_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(xingce_all_ques_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(xingce_all_time_total.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(xingce_all_correct_num.value))
          } else if (subject == 2) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(gongji_all_user_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(gongji_all_ques_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(gongji_all_time_total.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(gongji_all_correct_num.value))
          } else if (subject == 3) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(zhice_all_user_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(zhice_all_ques_count.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(zhice_all_time_total.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(zhice_all_correct_num.value))
          }

          buffer += Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }
    ts_hbasePar.saveAsHadoopDataset(total_station_jobConf)
    sparkSession.stop()
  }

}