package com.li.ability.assessment

import java.io.File

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

case class AnswerCard(userId: Long,
                      corrects: Seq[Int],
                      questions: Seq[Int],
                      times: Seq[Int],
                      points: Seq[Int],
                      createTime: String,
                      subject: Int)

case class TS_AbilityAssessment(userId: Long,
                                total_station_grade: Double,
                                total_station_predict_score: String,
                                do_exercise_num: Long,
                                cumulative_time: Long,
                                do_exercise_day: Long,
                                subject: Int,
                                total_correct_num: Long,
                                total_undo_num: Long
                               )

case class Week_AbilityAssessment(userId: Long,
                                  week_grade: Double,
                                  week_predict_score: String,
                                  subject: Int,
                                  week_do_exercise_num: Long,
                                  week_cumulative_time: Long,
                                  week_correct_num: Long,
                                  week_speek: Double,
                                  week_accuracy: Double,
                                  week_undo_num: Long

                                 )

object AbilityAssessment3 {

  def main(args: Array[String]): Unit = {


    var hive_input_table = "ztk_answer_card"
    var weekTop10Table = "week_top10_ability_assessment"
    var weekTable = "week_ability_assessment"
    var hbase_output_table = "total_station_ability_assessment"
    var mysql = "jdbc:mysql://192.168.100.18/pandora?characterEncoding=UTF-8&transformedBitIsBoolean=false&tinyInt1isBit=false"
    var user = "vhuatu"
    var password = "vhuatu_2013"

    if (args.length == 4) {
      hive_input_table = args(0)
      weekTop10Table = args(1)
      weekTable = args(2)
      hbase_output_table = args(3)
      mysql = "jdbc:mysql://192.168.100.21/teacher?characterEncoding=UTF-8&transformedBitIsBoolean=false&tinyInt1isBit=false"
      user = "root"
      password = "unimob@12254ns"
    }

//        hive_input_table = "zac2"
//        weekTop10Table = "test_week_top10_ability_assessment"
//        weekTable = "test_week_ability_assessment"
//        hbase_output_table = "test_total_station_ability_assessment"
//        mysql = "jdbc:mysql://192.168.100.21/teacher?characterEncoding=UTF-8&transformedBitIsBoolean=false&tinyInt1isBit=false"
//        user = "root"
//        password = "unimob@12254ns"


    System.setProperty("HADOOP_USER_NAME", "root")
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val conf = new SparkConf()
      .setAppName("AbilityAssessment3")
//      .setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]], classOf[AnswerCard]))

    val sparkSession = SparkSession.builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    import sparkSession.implicits._
    val sc = sparkSession.sparkContext

    var options: Map[String, String] = Map(
      "url" -> mysql,
      "dbtable" -> "knowledge",
      "user" -> user,
      "password" -> password
    )
    // v_knowledge_point
    val v_knowledge_point = sparkSession.read.format("jdbc").options(options).load
    v_knowledge_point.createOrReplaceTempView("knowledge")

    // v_new_subject
    options += ("dbtable" -> "knowledge_subject")
    val v_new_subject = sparkSession.read.format("jdbc").options(options).load
    v_new_subject.createOrReplaceTempView("knowledge_subject")

    val vkp = sparkSession.sql(" " +
      " SELECT  " +
      " knowledge_subject.subject_id,knowledge.`name`,knowledge.id " +
      " FROM" +
      "   knowledge" +
      " LEFT JOIN knowledge_subject ON knowledge.id = knowledge_subject.knowledge_id" +
      " AND knowledge.`status` = 1" +
      " AND knowledge_subject.`status` = 1" +
      " WHERE" +
      "   knowledge.`level` = 1 ").rdd


    vkp.cache()

    val subjetPoint = sc.broadcast(vkp.mapPartitions {
      ite: Iterator[Row] =>

        val arr = new ArrayBuffer[(Int, String)]()

        while (ite.hasNext) {
          val r = ite.next()

          arr += Tuple2(r.getAs[Int](0), r.getAs[Long](2).toString)
        }
        arr.iterator
    }.reduceByKey(_ + "," + _).collectAsMap())

    val subjetPointName = sc.broadcast(vkp.mapPartitions {
      ite: Iterator[Row] =>

        val arr = new ArrayBuffer[(Int, String)]()

        while (ite.hasNext) {
          val r = ite.next()

          arr += Tuple2(r.getAs[Int](0), r.getAs[String](1))
        }
        arr.iterator
    }.reduceByKey(_ + "," + _).collectAsMap())


    sparkSession.udf.register("predictedScore", new PredictedScore)
    val predicted_score = sparkSession.sql("" +
      " select userId,predictedScore(correct,question,answerTime,point,createTime) predictedScore,subject " +
      " from " + hive_input_table + " " +
      //      " where createTime='20181211' " +
      " group by userId,subject")

    /**
      * 全站
      */
    // 统计科目下的用户数量
    val userCountXc = sc.longAccumulator("userCountXc")
    val userCountGj = sc.longAccumulator("userCountGj")
    val userCountZc = sc.longAccumulator("userCountZc")
    val userCountGa = sc.longAccumulator("userCountGa")
    // 统计科目下的做题数量
    val qCountXc = sc.longAccumulator("qCountXc")
    val qCountGj = sc.longAccumulator("qCountGj")
    val qCountZc = sc.longAccumulator("qCountZc")
    val qCountGa = sc.longAccumulator("qCountGa")
    // 统计科目下的做题时长
    val timeToltalXc = sc.longAccumulator("timeToltalXc")
    val timeTotalGj = sc.longAccumulator("timeTotalGj")
    val timeTotalZc = sc.longAccumulator("timeTotalZc")
    val timeTotalGa = sc.longAccumulator("timeTotalGa")
    // 统计科目下的正确数量
    val cNumXc = sc.longAccumulator("cNumXc")
    val cNumGj = sc.longAccumulator("cNumGj")
    val cNumZc = sc.longAccumulator("cNumZc")
    val cNumGa = sc.longAccumulator("cNumGa")

    val undoNumXc = sc.longAccumulator("undoNumXc")
    val undoNumGj = sc.longAccumulator("undoNumGj")
    val undoNumZc = sc.longAccumulator("undoNumZc")
    val undoNumGa = sc.longAccumulator("undoNumGa")

    /**
      * 周
      */
    // 周:统计科目下的用户数量
    val weekUCountXc = sc.longAccumulator("weekUCountXc")
    val weekUCountGj = sc.longAccumulator("weekUCountGj")
    val weekUCountZc = sc.longAccumulator("weekUCountZc")
    val weekUCountGa = sc.longAccumulator("weekUCountGa")

    // 周:统计科目下的做题数量
    val weekQCountXc = sc.longAccumulator("weekQCountXc")
    val weekQCountGj = sc.longAccumulator("weekQCountGj")
    val weekQCountZc = sc.longAccumulator("weekQCountZc")
    val weekQCountGa = sc.longAccumulator("weekQCountGa")

    // 周:统计科目下的做题时长
    val weekTTotalXc = sc.longAccumulator("weekTTotalXc")
    val weekTTotalGj = sc.longAccumulator("weekTTotalGj")
    val weekTTotalZc = sc.longAccumulator("weekTTotalZc")
    val weekTTotalGa = sc.longAccumulator("weekTTotalGa")

    // 周:统计科目下的正确数量
    val weekCNumXc = sc.longAccumulator("weekCNumXc")
    val weekCNumGj = sc.longAccumulator("weekCNumGj")
    val weekCNumZc = sc.longAccumulator("weekCNumZc")
    val weekCNumGa = sc.longAccumulator("weekCNumGa")

    val weekUndoNumXc = sc.longAccumulator("weekUndoNumXc")
    val weekUndoNumGj = sc.longAccumulator("weekUndoNumGj")
    val weekUndoNumZc = sc.longAccumulator("weekUndoNumZc")
    val weekUndoNumGa = sc.longAccumulator("weekUndoNumGa")

    predicted_score.cache()
    val ts_predicted_score_df = predicted_score.mapPartitions {
      ite =>
        var arr = new ArrayBuffer[TS_AbilityAssessment]()


        while (ite.hasNext) {
          val n = ite.next()
          val userId = n.get(0).asInstanceOf[Long].longValue()
          val predictedScore = n.get(1).asInstanceOf[Seq[String]].seq
          val subject = n.get(2).asInstanceOf[Int].intValue()

          if (subject == 1) {
            userCountXc.add(1)
            qCountXc.add(predictedScore(1).toLong)
            timeToltalXc.add(predictedScore(2).toLong)
            cNumXc.add(predictedScore(7).toLong)
            undoNumXc.add(predictedScore(9).toLong)
          } else if (subject == 2) {
            userCountGj.add(1)
            qCountGj.add(predictedScore(1).toLong)
            timeTotalGj.add(predictedScore(2).toLong)
            cNumGj.add(predictedScore(7).toLong)
            undoNumGj.add(predictedScore(9).toLong)
          } else if (subject == 3) {
            userCountZc.add(1)
            qCountZc.add(predictedScore(1).toLong)
            timeTotalZc.add(predictedScore(2).toLong)
            cNumZc.add(predictedScore(7).toLong)
            undoNumZc.add(predictedScore(9).toLong)
          }
          else if (subject == 100100175) {
            userCountGa.add(1)
            qCountGa.add(predictedScore(1).toLong)
            timeTotalGa.add(predictedScore(2).toLong)
            cNumGa.add(predictedScore(7).toLong)
            undoNumGa.add(predictedScore(9).toLong)
          }
          //          println(userCountXc.value)
          arr += TS_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore(0), subject), //total_station_grade: Double,
            predictedScore(0), //total_station_predict_score
            predictedScore(1).toLong, //do_exercise_num
            predictedScore(2).toLong, //cumulative_time
            predictedScore(4).toLong, //do_exercise_day
            subject,
            predictedScore(7).toLong, //total_correct_num
            predictedScore(9).toLong //total_undo_num
          )
        }
        arr.iterator
    }.toDF()
    println(ts_predicted_score_df.count())

    val _XcUCount = sc.broadcast(userCountXc.value.toString)
    val _XcQCount = sc.broadcast(qCountXc.value.toString)
    val _XcTTotal = sc.broadcast(timeToltalXc.value.toString)
    val _XcCNum = sc.broadcast(cNumXc.value.toString)
    val _XcUndoNum = sc.broadcast(undoNumXc.value.toString)

    val _GjUCount = sc.broadcast(userCountGj.value.toString)
    val _GjQCOUNT = sc.broadcast(qCountGj.value.toString)
    val _GjTTotal = sc.broadcast(timeTotalGj.value.toString)
    val _GjCNum = sc.broadcast(cNumGj.value.toString)
    val _GjUndoNum = sc.broadcast(undoNumGj.value.toString)

    val _ZcUCount = sc.broadcast(userCountZc.value.toString)
    val _ZcQCount = sc.broadcast(qCountZc.value.toString)
    val _ZcTTotal = sc.broadcast(timeTotalZc.value.toString)
    val _ZcCNumt = sc.broadcast(cNumZc.value.toString)
    val _ZcUndoNum = sc.broadcast(undoNumZc.value.toString)

    val _GaUCount = sc.broadcast(userCountGa.value.toString)
    val _GaQCount = sc.broadcast(qCountGa.value.toString)
    val _GaTTotal = sc.broadcast(timeTotalGa.value.toString)
    val _GaCNumt = sc.broadcast(cNumGa.value.toString)
    val _GaUndoNum = sc.broadcast(undoNumGa.value.toString)

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

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.rootdir", "/hbase")
    hbaseConf.set("hbase.client.retries.number", "300")
    hbaseConf.set("hbase.rpc.timeout", "200000")
    hbaseConf.set("hbase.client.operation.timeout", "3000")
    hbaseConf.set("hbase.client.scanner.timeout.period", "10000")

    val total_station_jobConf = new JobConf(hbaseConf)
    total_station_jobConf.setOutputFormat(classOf[TableOutputFormat])
    total_station_jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbase_output_table)
    val ts_hbasePar = ts.rdd.mapPartitions {
      ite: Iterator[Row] =>

        var buffer = new ArrayBuffer[(ImmutableBytesWritable, Put)]()
        val sp = subjetPoint.value
        val spn = subjetPointName.value

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
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("sp"), Bytes.toBytes(sp.getOrElse(subject, "").toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("spn"), Bytes.toBytes(spn.getOrElse(subject, "")))


          if (subject == 1) {

            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(_XcUCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(_XcQCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_XcTTotal.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(_XcCNum.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("undoCount"), Bytes.toBytes(_XcUndoNum.value))
          } else if (subject == 2) {

            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(_GjUCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(_GjQCOUNT.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_GjTTotal.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(_GjCNum.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("undoCount"), Bytes.toBytes(_GjUndoNum.value))
          } else if (subject == 3) {

            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcUCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcQCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_ZcTTotal.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcCNumt.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("undoCount"), Bytes.toBytes(_ZcUndoNum.value))
          } else if (subject == 100100175) {

            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(_GaUCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(_GaQCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_GaTTotal.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(_GaCNumt.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("undoCount"), Bytes.toBytes(_GaUndoNum.value))
          }

          buffer += Tuple2(new ImmutableBytesWritable, put)
        }
        buffer.iterator
    }
    ts_hbasePar.saveAsHadoopDataset(total_station_jobConf)


    val week_predicted_score_df = predicted_score.mapPartitions {
      ite =>
        var arr = new ArrayBuffer[Week_AbilityAssessment]()
        while (ite.hasNext) {
          val n = ite.next()
          val userId = n.get(0).asInstanceOf[Long].longValue()
          val predictedScore = n.get(1).asInstanceOf[Seq[String]].seq
          val subject = n.get(2).asInstanceOf[Int].intValue()

          if (subject == 1) {
            weekUCountXc.add(1)
            weekQCountXc.add(predictedScore(1).toLong)
            weekTTotalXc.add(predictedScore(2).toLong)
            weekCNumXc.add(predictedScore(8).toLong)
          } else if (subject == 2) {
            weekUCountGj.add(1)
            weekQCountGj.add(predictedScore(1).toLong)
            weekTTotalGj.add(predictedScore(2).toLong)
            weekCNumGj.add(predictedScore(8).toLong)
          } else if (subject == 3) {
            weekUCountZc.add(1)
            weekQCountZc.add(predictedScore(1).toLong)
            weekTTotalZc.add(predictedScore(2).toLong)
            weekCNumZc.add(predictedScore(8).toLong)
          } else if (subject == 100100175) {
            weekUCountGa.add(1)
            weekQCountGa.add(predictedScore(1).toLong)
            weekTTotalGa.add(predictedScore(2).toLong)
            weekCNumGa.add(predictedScore(8).toLong)
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
            predictedScore(8).toLong * 1.0 / predictedScore(5).toLong,
            predictedScore(10).toLong
          )
        }
        arr.iterator
    }.toDF()
    println(week_predicted_score_df.count())

    val _XcWeekUCount = sc.broadcast(weekUCountXc.value.toString)
    val _XcWeekQCount = sc.broadcast(weekQCountXc.value.toString)
    val _XcWeekTTotal = sc.broadcast(weekTTotalXc.value.toString)
    val _XcWeekCCnum = sc.broadcast(weekCNumXc.value.toString)
    val _XcWeekUndonum = sc.broadcast(weekUndoNumXc.value.toString)

    val _GjWeekUCount = sc.broadcast(weekUCountGj.value.toString)
    val _GjWeekQCount = sc.broadcast(weekQCountGj.value.toString)
    val _GjWeekTTotal = sc.broadcast(weekTTotalGj.value.toString)
    val _GjWeekCNum = sc.broadcast(weekCNumGj.value.toString)
    val _GjWeekUndonum = sc.broadcast(weekUndoNumGj.value.toString)


    val _ZcWeekUCount = sc.broadcast(weekUCountZc.value.toString)
    val _ZcWeekQCount = sc.broadcast(weekQCountZc.value.toString)
    val _ZcWeekTTotal = sc.broadcast(weekTTotalZc.value.toString)
    val _ZcWeekCNum = sc.broadcast(weekCNumZc.value.toString)
    val _ZcWeekUndonum = sc.broadcast(weekUndoNumZc.value.toString)

    val _GaWeekUCount = sc.broadcast(weekUCountGa.value.toString)
    val _GaWeekQCount = sc.broadcast(weekQCountGa.value.toString)
    val _GaWeekTTotal = sc.broadcast(weekTTotalGa.value.toString)
    val _GaWeekCNum = sc.broadcast(weekCNumGa.value.toString)
    val _GaWeekUndonum = sc.broadcast(weekUndoNumGa.value.toString)

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
    week_top10_hbaseConf.set("hbase.rpc.timeout", "20000")
    week_top10_hbaseConf.set("hbase.client.operation.timeout", "3000")
    week_top10_hbaseConf.set("hbase.client.scanner.timeout.period", "10000")

    val week_top10_jobConf = new JobConf(week_top10_hbaseConf)
    week_top10_jobConf.setOutputFormat(classOf[TableOutputFormat])
    week_top10_jobConf.set(TableOutputFormat.OUTPUT_TABLE, weekTop10Table)
    val week_top10_hbasePar = weekTop10.rdd.mapPartitions {
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
        }
        buffer.iterator
    }

    week_top10_hbasePar.saveAsHadoopDataset(week_top10_jobConf)


    val week_hbaseConf = HBaseConfiguration.create()
    week_hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    week_hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    week_hbaseConf.set("hbase.rootdir", "/hbase")
    week_hbaseConf.set("hbase.client.retries.number", "300")
    week_hbaseConf.set("hbase.rpc.timeout", "200000")
    week_hbaseConf.set("hbase.client.operation.timeout", "3000")
    week_hbaseConf.set("hbase.client.scanner.timeout.period", "10000")
    val week_jobConf = new JobConf(week_hbaseConf)
    week_jobConf.setOutputFormat(classOf[TableOutputFormat])
    week_jobConf.set(TableOutputFormat.OUTPUT_TABLE, weekTable)
    val week_hbasePar = week.rdd.mapPartitions {
      ite: Iterator[Row] =>

        var buffer = new ArrayBuffer[(ImmutableBytesWritable, Put)]()
        val sp = subjetPoint.value
        val spn = subjetPointName.value
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
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("sp"), Bytes.toBytes(sp.getOrElse(subject, "").toString))
          put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("spn"), Bytes.toBytes(spn.getOrElse(subject, "")))

          if (subject == 1) {

            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(_XcWeekUCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(_XcWeekQCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("timeTotal"), Bytes.toBytes(_XcWeekTTotal.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(_XcWeekCCnum.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("undoNum"), Bytes.toBytes(_XcWeekUndonum.value))
          } else if (subject == 2) {

            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(_GjWeekUCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(_GjWeekQCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("timeTotal"), Bytes.toBytes(_GjWeekTTotal.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(_GjWeekCNum.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("undoNum"), Bytes.toBytes(_GjWeekUndonum.value))
          } else if (subject == 3) {

            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcWeekUCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcWeekQCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("timeTotal"), Bytes.toBytes(_ZcWeekTTotal.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcWeekCNum.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("undoNum"), Bytes.toBytes(_ZcWeekUndonum.value))
          }
          else if (subject == 100100175) {

            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("userCount"), Bytes.toBytes(_GaWeekUCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("quesCount"), Bytes.toBytes(_GaWeekQCount.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("timeTotal"), Bytes.toBytes(_GaWeekTTotal.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("correctNum"), Bytes.toBytes(_GaWeekCNum.value))
            put.addColumn(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("undoNum"), Bytes.toBytes(_GaWeekUndonum.value))
          }

          buffer += Tuple2(new ImmutableBytesWritable, put)
        }
        buffer.iterator
    }

    week_hbasePar.saveAsHadoopDataset(week_jobConf)


    sparkSession.stop()
  }

}