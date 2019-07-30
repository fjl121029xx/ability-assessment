package com.li.ability.assessment

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.li.ability.assessment.udaf.PredictedScore
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{Row, SparkSession}

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
                                total_undo_num: Long,
                                sortScore: Double
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
                                  week_undo_num: Long,
                                  sortScore: Double
                                 )

object AbilityAssessment3 {

  def main(args: Array[String]): Unit = {


    var dataSource = "ztk_answer_card2"

    var t_all = "total_station_ability_assessment"
    var t_week = "week_ability_assessment"
    var t_weekTop = "week_top10_ability_assessment"
    var t_family = "ability_assessment_info"

    var mysql = "jdbc:mysql://192.168.100.154/pandora?characterEncoding=UTF-8&transformedBitIsBoolean=false&tinyInt1isBit=false"
    var user = "vhuatu"
    var password = "vhuatu_2013"

    if (args.length == 5) {
      dataSource = args(0)
      t_weekTop = args(1)
      t_week = args(2)
      t_all = args(3)
      t_family = args(4)
      if (dataSource.eq("zac2")) {
        mysql = "jdbc:mysql://192.168.100.21/teacher?characterEncoding=UTF-8&transformedBitIsBoolean=false&tinyInt1isBit=false"
        user = "root"
        password = "unimob@12254ns"
      }

    }
    //    //
    //    dataSource = "zac2"
    //    t_weekTop = "test_week_top10_ability_assessment"
    //    t_week = "test_week_ability_assessment"
    //    t_all = "test_total_station_ability_assessment"
    //    mysql = "jdbc:mysql://192.168.100.21/teacher?characterEncoding=UTF-8&transformedBitIsBoolean=false&tinyInt1isBit=false"
    //    user = "root"
    //    password = "unimob@12254ns"


    System.setProperty("HADOOP_USER_NAME", "root")
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val conf = new SparkConf()
      .setAppName("AbilityAssessment3")
      //                  .setMaster("local[3]")
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

    println(subjetPoint.value)

    val subjetPointName = sc.broadcast(vkp.mapPartitions {
      ite: Iterator[Row] =>

        val arr = new ArrayBuffer[(Int, String)]()

        while (ite.hasNext) {
          val r = ite.next()

          arr += Tuple2(r.getAs[Int](0), r.getAs[String](1))
        }
        arr.iterator
    }.reduceByKey(_ + "," + _).collectAsMap())
    println(subjetPointName.value)


    val sdf = new SimpleDateFormat("yyyyMMdd")
    val month = System.currentTimeMillis() - 30 * 24 * 60 * 60 * 1000L
    //
    //    val blus = sparkSession.sql("" +
    //      " select distinct userId  " +
    //      " from " + hive_input_table + "" +
    //      " where createtime >= " + sdf.format(new Date(month)))
    //
    //    blus.show()

    val blackUser = sc.broadcast(sparkSession.sql("" +
      " select distinct userId  " +
      " from " + dataSource + "" +
      " where createtime >= " + sdf.format(new Date(month))).rdd.mapPartitions {

      ite: Iterator[Row] =>
        val arr = new ArrayBuffer[Long]()

        while (ite.hasNext) {

          val line = ite.next()
          arr += line.getAs[Long](0)
        }
        arr.iterator
    }.collect())
    println(blackUser.value)

    import sparkSession.implicits._

    sparkSession.udf.register("predictedScore", new PredictedScore)
    val predicted_score = sparkSession.sql("" +
      " select userId,predictedScore(correct,question,answerTime,point,createTime) predictedScore,subject " +
      " from " + dataSource + " " +
      //      " where createTime<='20181230' and createTime>='20181224' and userId=234964707 and subject=2" +
      " group by userId,subject")
      .filter {
        r =>
          val userid = r.get(0).asInstanceOf[Long].longValue()
          val predictedScore = r.get(1).asInstanceOf[Seq[String]].seq
          val exeNum = predictedScore(1).toLong
          val bu = blackUser.value
          if (exeNum >= 1) {
            true
          } else {
            false
          }

          if (bu.contains(userid)) {
            true
          } else {
            false
          }
      }
      .coalesce(500)

    /**
      * 全站
      */
    // 统计科目下的用户数量
    val userCountXc = sc.longAccumulator("userCountXc")
    val userCountGj = sc.longAccumulator("userCountGj")
    val userCountZcB = sc.longAccumulator("userCountZcB")
    val userCountGa = sc.longAccumulator("userCountGa")
    // 合格用户
    val qualifiedUserCountXc = sc.longAccumulator("qualifiedUserCountXc")
    val qualifiedUserCountGj = sc.longAccumulator("qualifiedUserCountGj")
    val qualifiedUserCountZcB = sc.longAccumulator("qualifiedUserCountZcB")
    val qualifiedUserCountGa = sc.longAccumulator("qualifiedUserCountGa")
    // 统计科目下的做题数量
    val qCountXc = sc.longAccumulator("qCountXc")
    val qCountGj = sc.longAccumulator("qCountGj")
    val qCountZcB = sc.longAccumulator("qCountZcB")
    val qCountGa = sc.longAccumulator("qCountGa")
    // 统计科目下的做题时长
    val timeToltalXc = sc.longAccumulator("timeToltalXc")
    val timeTotalGj = sc.longAccumulator("timeTotalGj")
    val timeTotalZcB = sc.longAccumulator("timeTotalZcB")
    val timeTotalGa = sc.longAccumulator("timeTotalGa")
    // 统计科目下的正确数量
    val cNumXc = sc.longAccumulator("cNumXc")
    val cNumGj = sc.longAccumulator("cNumGj")
    val cNumZcB = sc.longAccumulator("cNumZcB")
    val cNumGa = sc.longAccumulator("cNumGa")

    val undoNumXc = sc.longAccumulator("undoNumXc")
    val undoNumGj = sc.longAccumulator("undoNumGj")
    val undoNumZcB = sc.longAccumulator("undoNumZcB")
    val undoNumGa = sc.longAccumulator("undoNumGa")


    val userCountZcC = sc.longAccumulator("userCountZcC")
    val qualifiedUserCountZcC = sc.longAccumulator("qualifiedUserCountZcC")
    val qCountZcC = sc.longAccumulator("qCountZcC")
    val timeTotalZcC = sc.longAccumulator("timeTotalZcC")
    val cNumZcC = sc.longAccumulator("cNumZcC")
    val undoNumZcC = sc.longAccumulator("undoNumZcC")

    val userCountZcA = sc.longAccumulator("userCountZcA")
    val qualifiedUserCountZcA = sc.longAccumulator("qualifiedUserCountZcA")
    val qCountZcA = sc.longAccumulator("qCountZcA")
    val timeTotalZcA = sc.longAccumulator("timeTotalZcA")
    val cNumZcA = sc.longAccumulator("cNumZcA")
    val undoNumZcA = sc.longAccumulator("undoNumZcA")

    val userCountZcD = sc.longAccumulator("userCountZcD")
    val qualifiedUserCountZcD = sc.longAccumulator("qualifiedUserCountZcD")
    val qCountZcD = sc.longAccumulator("qCountZcD")
    val timeTotalZcD = sc.longAccumulator("timeTotalZcD")
    val cNumZcD = sc.longAccumulator("cNumZcD")
    val undoNumZcD = sc.longAccumulator("undoNumZcD")
    /**
      * 周
      */
    // 周:统计科目下的用户数量
    val weekUCountXc = sc.longAccumulator("weekUCountXc")
    val weekUCountGj = sc.longAccumulator("weekUCountGj")
    val weekUCountZcB = sc.longAccumulator("weekUCountZcB")
    val weekUCountGa = sc.longAccumulator("weekUCountGa")

    // 周:统计科目下的做题数量
    val weekQCountXc = sc.longAccumulator("weekQCountXc")
    val weekQCountGj = sc.longAccumulator("weekQCountGj")
    val weekQCountZcB = sc.longAccumulator("weekQCountZcB")
    val weekQCountGa = sc.longAccumulator("weekQCountGa")

    // 周:统计科目下的做题时长
    val weekTTotalXc = sc.longAccumulator("weekTTotalXc")
    val weekTTotalGj = sc.longAccumulator("weekTTotalGj")
    val weekTTotalZcB = sc.longAccumulator("weekTTotalZcB")
    val weekTTotalGa = sc.longAccumulator("weekTTotalGa")

    // 周:统计科目下的正确数量
    val weekCNumXc = sc.longAccumulator("weekCNumXc")
    val weekCNumGj = sc.longAccumulator("weekCNumGj")
    val weekCNumZcB = sc.longAccumulator("weekCNumZcB")
    val weekCNumGa = sc.longAccumulator("weekCNumGa")

    val weekUndoNumXc = sc.longAccumulator("weekUndoNumXc")
    val weekUndoNumGj = sc.longAccumulator("weekUndoNumGj")
    val weekUndoNumZcB = sc.longAccumulator("weekUndoNumZcB")
    val weekUndoNumGa = sc.longAccumulator("weekUndoNumGa")

    val weekUCountZcA = sc.longAccumulator("weekUCountZcA")
    val weekQCountZcA = sc.longAccumulator("weekQCountZcA")
    val weekTTotalZcA = sc.longAccumulator("weekTTotalZcA")
    val weekCNumZcA = sc.longAccumulator("weekCNumZcA")
    val weekUndoNumZcA = sc.longAccumulator("weekUndoNumZcA")

    val weekUCountZcC = sc.longAccumulator("weekUCountZcC")
    val weekQCountZcC = sc.longAccumulator("weekQCountZcC")
    val weekTTotalZcC = sc.longAccumulator("weekTTotalZcC")
    val weekCNumZcC = sc.longAccumulator("weekCNumZcC")
    val weekUndoNumZcC = sc.longAccumulator("weekUndoNumZcC")


    val weekUCountZcD = sc.longAccumulator("weekUCountZcD")
    val weekQCountZcD = sc.longAccumulator("weekQCountZcD")
    val weekTTotalZcD = sc.longAccumulator("weekTTotalZcD")
    val weekCNumZcD = sc.longAccumulator("weekCNumZcD")
    val weekUndoNumZcD = sc.longAccumulator("weekUndoNumZcD")


    predicted_score.cache()

    val ts_predicted_score_df = predicted_score.mapPartitions {
      ite =>
        var arr = new ArrayBuffer[S_C_A_A]()
        val m = subjetPointName.value

        while (ite.hasNext) {
          val n = ite.next()
          val userId = n.get(0).asInstanceOf[Long].longValue()
          val predictedScore = n.get(1).asInstanceOf[Seq[String]].seq
          val subject = n.get(2).asInstanceOf[Int].intValue()

          val score = PredictedScore.getScore(predictedScore(0), subject, 0) //total_station_grade: Double,
          val exeNum = predictedScore(1).toLong
          val exeTime = predictedScore(2).toLong

          val speed = exeTime * 1.0 / exeNum

          var sortScore = score
          if (score < 20.0) {
            sortScore = 0.00
          }

          if (subject == 1) {
            userCountXc.add(1)
            qCountXc.add(predictedScore(1).toLong)
            timeToltalXc.add(predictedScore(2).toLong)
            cNumXc.add(predictedScore(7).toLong)
            undoNumXc.add(predictedScore(9).toLong)

            if (score >= 20.0) {
              qualifiedUserCountXc.add(1)
            }
          } else if (subject == 2) {
            userCountGj.add(1)
            qCountGj.add(predictedScore(1).toLong)
            timeTotalGj.add(predictedScore(2).toLong)
            cNumGj.add(predictedScore(7).toLong)
            undoNumGj.add(predictedScore(9).toLong)

            if (score >= 20.0) {
              qualifiedUserCountGj.add(1)
            }
          } else if (subject == 200100055) {
            userCountZcB.add(1)
            qCountZcB.add(predictedScore(1).toLong)
            timeTotalZcB.add(predictedScore(2).toLong)
            cNumZcB.add(predictedScore(7).toLong)
            undoNumZcB.add(predictedScore(9).toLong)

            if (score >= 20.0) {
              qualifiedUserCountZcB.add(1)
            }
          } else if (subject == 200100054) {
            userCountZcA.add(1)
            qCountZcA.add(predictedScore(1).toLong)
            timeTotalZcA.add(predictedScore(2).toLong)
            cNumZcA.add(predictedScore(7).toLong)
            undoNumZcA.add(predictedScore(9).toLong)

            if (score >= 20.0) {
              qualifiedUserCountZcA.add(1)
            }
          } else if (subject == 200100056) {
            userCountZcC.add(1)
            qCountZcC.add(predictedScore(1).toLong)
            timeTotalZcC.add(predictedScore(2).toLong)
            cNumZcC.add(predictedScore(7).toLong)
            undoNumZcC.add(predictedScore(9).toLong)

            if (score >= 20.0) {
              qualifiedUserCountZcC.add(1)
            }
          } else if (subject == 200100057) {
            userCountZcD.add(1)
            qCountZcD.add(predictedScore(1).toLong)
            timeTotalZcD.add(predictedScore(2).toLong)
            cNumZcD.add(predictedScore(7).toLong)
            undoNumZcD.add(predictedScore(9).toLong)

            if (score >= 20.0) {
              qualifiedUserCountZcD.add(1)
            }
          }
          else if (subject == 100100175) {
            userCountGa.add(1)
            qCountGa.add(predictedScore(1).toLong)
            timeTotalGa.add(predictedScore(2).toLong)
            cNumGa.add(predictedScore(7).toLong)
            undoNumGa.add(predictedScore(9).toLong)

            if (score >= 20.0) {
              qualifiedUserCountGa.add(1)
            }
          }
          //          println(userCountXc.value)
          arr += S_C_A_A(
            userId, //userId
            score, //total_station_grade: Double,
            predictedScore(0), //total_station_predict_score
            exeNum, //do_exercise_num
            exeTime, //cumulative_time
            predictedScore(4).toLong, //do_exercise_day
            subject,
            predictedScore(7).toLong, //total_correct_num
            predictedScore(9).toLong, //total_undo_num
            sortScore
          )
        }
        arr.iterator
    }.coalesce(500).toDF()

    ts_predicted_score_df.count()

    val _XcUCount = sc.broadcast(userCountXc.value.toString)
    val _XcQUCount = sc.broadcast(qualifiedUserCountXc.value.toString)
    val _XcQCount = sc.broadcast(qCountXc.value.toString)
    val _XcTTotal = sc.broadcast(timeToltalXc.value.toString)
    val _XcCNum = sc.broadcast(cNumXc.value.toString)
    val _XcUndoNum = sc.broadcast(undoNumXc.value.toString)

    val _GjUCount = sc.broadcast(userCountGj.value.toString)
    val _GjQUCount = sc.broadcast(qualifiedUserCountGj.value.toString)
    val _GjQCOUNT = sc.broadcast(qCountGj.value.toString)
    val _GjTTotal = sc.broadcast(timeTotalGj.value.toString)
    val _GjCNum = sc.broadcast(cNumGj.value.toString)
    val _GjUndoNum = sc.broadcast(undoNumGj.value.toString)

    val _ZcUCountB = sc.broadcast(userCountZcB.value.toString)
    val _ZcQUCountB = sc.broadcast(qualifiedUserCountZcB.value.toString)
    val _ZcQCountB = sc.broadcast(qCountZcB.value.toString)
    val _ZcTTotalB = sc.broadcast(timeTotalZcB.value.toString)
    val _ZcCNumtB = sc.broadcast(cNumZcB.value.toString)
    val _ZcUndoNumB = sc.broadcast(undoNumZcB.value.toString)

    val _ZcUCountA = sc.broadcast(userCountZcA.value.toString)
    val _ZcQUCountA = sc.broadcast(qualifiedUserCountZcA.value.toString)
    val _ZcQCountA = sc.broadcast(qCountZcA.value.toString)
    val _ZcTTotalA = sc.broadcast(timeTotalZcA.value.toString)
    val _ZcCNumtA = sc.broadcast(cNumZcA.value.toString)
    val _ZcUndoNumA = sc.broadcast(undoNumZcA.value.toString)

    val _ZcUCountC = sc.broadcast(userCountZcC.value.toString)
    val _ZcQUCountC = sc.broadcast(qualifiedUserCountZcC.value.toString)
    val _ZcQCountC = sc.broadcast(qCountZcC.value.toString)
    val _ZcTTotalC = sc.broadcast(timeTotalZcC.value.toString)
    val _ZcCNumtC = sc.broadcast(cNumZcC.value.toString)
    val _ZcUndoNumC = sc.broadcast(undoNumZcC.value.toString)

    val _ZcUCountD = sc.broadcast(userCountZcD.value.toString)
    val _ZcQUCountD = sc.broadcast(qualifiedUserCountZcD.value.toString)
    val _ZcQCountD = sc.broadcast(qCountZcD.value.toString)
    val _ZcTTotalD = sc.broadcast(timeTotalZcD.value.toString)
    val _ZcCNumtD = sc.broadcast(cNumZcD.value.toString)
    val _ZcUndoNumD = sc.broadcast(undoNumZcD.value.toString)

    val _GaUCount = sc.broadcast(userCountGa.value.toString)
    val _GaQUCount = sc.broadcast(qualifiedUserCountGa.value.toString)
    val _GaQCount = sc.broadcast(qCountGa.value.toString)
    val _GaTTotal = sc.broadcast(timeTotalGa.value.toString)
    val _GaCNumt = sc.broadcast(cNumGa.value.toString)
    val _GaUndoNum = sc.broadcast(undoNumGa.value.toString)

    ts_predicted_score_df.createOrReplaceTempView("ts_predicted_score_df")

    val numGroupByExeDay = sc.broadcast(sparkSession.sql("" +
      " select  do_exercise_day as exeDay,count(*) " +
      "from ts_predicted_score_df group by do_exercise_day order by do_exercise_day")
      .rdd.mapPartitions {
      ite: Iterator[Row] =>
        val arr = new ArrayBuffer[(Long, Long)]()

        while (ite.hasNext) {

          val line = ite.next()

          arr += Tuple2(line.getAs[Long](0), line.getAs[Long](1))
        }

        arr.iterator
    }.collectAsMap())

    println(numGroupByExeDay.value)

    val ts = sparkSession.sql("" +
      " select " +
      "userId," +
      "total_station_grade," +
      "total_station_predict_score," +
      "do_exercise_num," +
      "cumulative_time," +
      "do_exercise_day," +
      "subject," +
      "Row_Number() OVER(partition by subject order by sortScore desc) rank," +
      "Row_Number() OVER(partition by subject order by do_exercise_day desc) rank2," +
      "Row_Number() OVER(partition by subject order by cumulative_time desc) rank3," +
      "Row_Number() OVER(partition by subject order by do_exercise_num desc) rank4, " +
      "sortScore," +
      "Row_Number() OVER(partition by subject order by total_station_grade desc) rank5 " +
      "from ts_predicted_score_df")


    val tsTop10 = ts.where("rank <= 1000")
    //    tsTop10.show(30000)


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
    total_station_jobConf.set(TableOutputFormat.OUTPUT_TABLE, t_all)
    val ts_hbasePar = ts.rdd.mapPartitions {
      ite: Iterator[Row] =>

        var buffer = new ArrayBuffer[(ImmutableBytesWritable, Put)]()
        val sp = subjetPoint.value
        val spn = subjetPointName.value

        val m = numGroupByExeDay.value


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
          val sortScore = t.get(11).asInstanceOf[Double].doubleValue()
          val rank5 = t.get(12).asInstanceOf[Int].intValue()

          var count: Long = 0L
          val passMan = m.foreach {
            case (a: Long, b: Long) => {
              if (do_exercise_day > a) {
                count += b;
              }
            }
          }


          val put = new Put(Bytes.toBytes(userId.toString + "-" + subject)) //行健的值
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("grade"), Bytes.toBytes(total_station_grade.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("predictScore"), Bytes.toBytes(total_station_predict_score.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseNum"), Bytes.toBytes(do_exercise_num.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTime"), Bytes.toBytes(cumulative_time.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseDay"), Bytes.toBytes(do_exercise_day.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank2"), Bytes.toBytes(rank2.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank3"), Bytes.toBytes(rank3.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank4"), Bytes.toBytes(rank4.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("sp"), Bytes.toBytes(sp.getOrElse(subject, "").toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("spn"), Bytes.toBytes(spn.getOrElse(subject, "")))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exeDayPassMan"), Bytes.toBytes(count.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("sortScore"), Bytes.toBytes(sortScore.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank5"), Bytes.toBytes(rank5.toString))


          if (subject == 1) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_XcUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("qualifiedUserCount"), Bytes.toBytes(_XcQUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_XcQCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_XcTTotal.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_XcCNum.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoCount"), Bytes.toBytes(_XcUndoNum.value))
          } else if (subject == 2) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_GjUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("qualifiedUserCount"), Bytes.toBytes(_GjQUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_GjQCOUNT.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_GjTTotal.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_GjCNum.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoCount"), Bytes.toBytes(_GjUndoNum.value))
          } else if (subject == 200100055) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcUCountB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("qualifiedUserCount"), Bytes.toBytes(_ZcQUCountB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcQCountB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_ZcTTotalB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcCNumtB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoCount"), Bytes.toBytes(_ZcUndoNumB.value))
          } else if (subject == 200100054) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcUCountA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("qualifiedUserCount"), Bytes.toBytes(_ZcQUCountA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcQCountA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_ZcTTotalA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcCNumtA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoCount"), Bytes.toBytes(_ZcUndoNumA.value))
          } else if (subject == 200100056) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcUCountC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("qualifiedUserCount"), Bytes.toBytes(_ZcQUCountC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcQCountC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_ZcTTotalC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcCNumtC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoCount"), Bytes.toBytes(_ZcUndoNumC.value))
          } else if (subject == 200100057) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcUCountD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("qualifiedUserCount"), Bytes.toBytes(_ZcQUCountD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcQCountD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_ZcTTotalD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcCNumtD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoCount"), Bytes.toBytes(_ZcUndoNumD.value))
          } else if (subject == 100100175) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_GaUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("qualifiedUserCount"), Bytes.toBytes(_GaQUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_GaQCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTimeTotal"), Bytes.toBytes(_GaTTotal.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_GaCNumt.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoCount"), Bytes.toBytes(_GaUndoNum.value))
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

          val exeNum = predictedScore(5).toLong
          val exeTime = predictedScore(6).toLong
          var sortScore = PredictedScore.getScore(predictedScore(3), subject, 1)
          val speed = predictedScore(6).toLong * 1.0 / predictedScore(5).toLong

          val allExeNum = predictedScore(1).toLong
          val allExeTime = predictedScore(2).toLong

          if (exeNum > 0) {
            if (subject == 1) {
              weekUCountXc.add(1)
              weekQCountXc.add(predictedScore(5).toLong)
              weekTTotalXc.add(predictedScore(6).toLong)
              weekCNumXc.add(predictedScore(8).toLong)
            } else if (subject == 2) {
              weekUCountGj.add(1)
              weekQCountGj.add(predictedScore(5).toLong)
              weekTTotalGj.add(predictedScore(6).toLong)
              weekCNumGj.add(predictedScore(8).toLong)
            } else if (subject == 200100055) {
              weekUCountZcB.add(1)
              weekQCountZcB.add(predictedScore(5).toLong)
              weekTTotalZcB.add(predictedScore(6).toLong)
              weekCNumZcB.add(predictedScore(8).toLong)
            } else if (subject == 200100054) {
              weekUCountZcA.add(1)
              weekQCountZcA.add(predictedScore(5).toLong)
              weekTTotalZcA.add(predictedScore(6).toLong)
              weekCNumZcA.add(predictedScore(8).toLong)
            } else if (subject == 200100056) {
              weekUCountZcC.add(1)
              weekQCountZcC.add(predictedScore(5).toLong)
              weekTTotalZcC.add(predictedScore(6).toLong)
              weekCNumZcC.add(predictedScore(8).toLong)
            } else if (subject == 200100057) {
              weekUCountZcD.add(1)
              weekQCountZcD.add(predictedScore(5).toLong)
              weekTTotalZcD.add(predictedScore(6).toLong)
              weekCNumZcD.add(predictedScore(8).toLong)
            } else if (subject == 100100175) {
              weekUCountGa.add(1)
              weekQCountGa.add(predictedScore(5).toLong)
              weekTTotalGa.add(predictedScore(6).toLong)
              weekCNumGa.add(predictedScore(8).toLong)
            }

            if (allExeNum < 300 || (allExeTime * 1.0) / allExeNum < 20.00) {
              sortScore = 0.00
            }

            arr += Week_AbilityAssessment(
              userId, //userId
              PredictedScore.getScore(predictedScore(3), subject, 1), //week_grade
              predictedScore(3), // week_predict_score
              subject,
              exeNum,
              exeTime,
              predictedScore(8).toLong,
              speed,
              predictedScore(8).toLong * 1.0 / predictedScore(5).toLong,
              predictedScore(10).toLong,
              sortScore
            )
          }


        }
        arr.iterator
    }.coalesce(500).toDF()

    week_predicted_score_df.count()

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


    val _ZcWeekUCountB = sc.broadcast(weekUCountZcB.value.toString)
    val _ZcWeekQCountB = sc.broadcast(weekQCountZcB.value.toString)
    val _ZcWeekTTotalB = sc.broadcast(weekTTotalZcB.value.toString)
    val _ZcWeekCNumB = sc.broadcast(weekCNumZcB.value.toString)
    val _ZcWeekUndonumB = sc.broadcast(weekUndoNumZcB.value.toString)

    val _ZcWeekUCountA = sc.broadcast(weekUCountZcA.value.toString)
    val _ZcWeekQCountA = sc.broadcast(weekQCountZcA.value.toString)
    val _ZcWeekTTotalA = sc.broadcast(weekTTotalZcA.value.toString)
    val _ZcWeekCNumA = sc.broadcast(weekCNumZcA.value.toString)
    val _ZcWeekUndonumA = sc.broadcast(weekUndoNumZcA.value.toString)

    val _ZcWeekUCountC = sc.broadcast(weekUCountZcC.value.toString)
    val _ZcWeekQCountC = sc.broadcast(weekQCountZcC.value.toString)
    val _ZcWeekTTotalC = sc.broadcast(weekTTotalZcC.value.toString)
    val _ZcWeekCNumC = sc.broadcast(weekCNumZcC.value.toString)
    val _ZcWeekUndonumC = sc.broadcast(weekUndoNumZcC.value.toString)

    val _ZcWeekUCountD = sc.broadcast(weekUCountZcD.value.toString)
    val _ZcWeekQCountD = sc.broadcast(weekQCountZcD.value.toString)
    val _ZcWeekTTotalD = sc.broadcast(weekTTotalZcD.value.toString)
    val _ZcWeekCNumD = sc.broadcast(weekCNumZcD.value.toString)
    val _ZcWeekUndonumD = sc.broadcast(weekUndoNumZcD.value.toString)


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
      "Row_Number() OVER(partition by subject order by sortScore desc) rank, " +
      "week_do_exercise_num," +
      "week_cumulative_time," +
      "Row_Number() OVER(partition by subject order by week_do_exercise_num desc) rank2," +
      "Row_Number() OVER(partition by subject order by week_speek desc) rank3," +
      "Row_Number() OVER(partition by subject order by week_accuracy desc) rank4," +
      "week_speek," +
      "week_accuracy," +
      "sortScore," +
      "Row_Number() OVER(partition by subject order by week_grade desc) rank5  " +
      "from week_predicted_score_df  ")


    val weekTop10 = week.where("rank <= 1000")
    //    weekTop10.show(30000)

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
    week_top10_jobConf.set(TableOutputFormat.OUTPUT_TABLE, t_weekTop)
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
          val sortScore = t.get(12).asInstanceOf[Double].doubleValue()


          //          val put = new Put(Bytes.toBytes(rank + "-" + subject + "-2018-52")) //行健的值
          val put = new Put(Bytes.toBytes(rank + "-" + subject + "-" + TimeUtils.getWeek())) //行健的值
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userId"), Bytes.toBytes(userId.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("grade"), Bytes.toBytes(grade.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("predict_score"), Bytes.toBytes(predictScore.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseNum"), Bytes.toBytes(exerciseNum.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTime"), Bytes.toBytes(exerciseTime.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("sortScore"), Bytes.toBytes(sortScore.toString))


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
    week_jobConf.set(TableOutputFormat.OUTPUT_TABLE, t_week)
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
          val sortScore = t.get(12).asInstanceOf[Double].doubleValue()
          val rank5 = t.get(13).asInstanceOf[Int].intValue()

          //          val put = new Put(Bytes.toBytes(userId + "-" + subject + "-2018-52")) //行健的值
          val put = new Put(Bytes.toBytes(userId + "-" + subject + "-" + TimeUtils.getWeek())) //行健的值
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("grade"), Bytes.toBytes(grade.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("predict_score"), Bytes.toBytes(predictScore.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseNum"), Bytes.toBytes(exerciseNum.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTime"), Bytes.toBytes(exerciseTime.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank2"), Bytes.toBytes(rank2.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank3"), Bytes.toBytes(rank3.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank4"), Bytes.toBytes(rank4.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("week_speek"), Bytes.toBytes(week_speek.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("week_accuracy"), Bytes.toBytes(week_accuracy.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("sp"), Bytes.toBytes(sp.getOrElse(subject, "").toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("spn"), Bytes.toBytes(spn.getOrElse(subject, "")))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("sortScore"), Bytes.toBytes(sortScore.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("rank5"), Bytes.toBytes(rank5.toString))

          if (subject == 1) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_XcWeekUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_XcWeekQCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("timeTotal"), Bytes.toBytes(_XcWeekTTotal.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_XcWeekCCnum.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoNum"), Bytes.toBytes(_XcWeekUndonum.value))
          } else if (subject == 2) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_GjWeekUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_GjWeekQCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("timeTotal"), Bytes.toBytes(_GjWeekTTotal.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_GjWeekCNum.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoNum"), Bytes.toBytes(_GjWeekUndonum.value))
          } else if (subject == 200100055) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcWeekUCountB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcWeekQCountB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("timeTotal"), Bytes.toBytes(_ZcWeekTTotalB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcWeekCNumB.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoNum"), Bytes.toBytes(_ZcWeekUndonumB.value))
          } else if (subject == 200100054) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcWeekUCountA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcWeekQCountA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("timeTotal"), Bytes.toBytes(_ZcWeekTTotalA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcWeekCNumA.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoNum"), Bytes.toBytes(_ZcWeekUndonumA.value))
          } else if (subject == 200100056) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcWeekUCountC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcWeekQCountC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("timeTotal"), Bytes.toBytes(_ZcWeekTTotalC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcWeekCNumC.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoNum"), Bytes.toBytes(_ZcWeekUndonumC.value))
          } else if (subject == 200100057) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_ZcWeekUCountD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_ZcWeekQCountD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("timeTotal"), Bytes.toBytes(_ZcWeekTTotalD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_ZcWeekCNumD.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoNum"), Bytes.toBytes(_ZcWeekUndonumD.value))
          } else if (subject == 100100175) {

            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("userCount"), Bytes.toBytes(_GaWeekUCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("quesCount"), Bytes.toBytes(_GaWeekQCount.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("timeTotal"), Bytes.toBytes(_GaWeekTTotal.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("correctNum"), Bytes.toBytes(_GaWeekCNum.value))
            put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("undoNum"), Bytes.toBytes(_GaWeekUndonum.value))
          }

          buffer += Tuple2(new ImmutableBytesWritable, put)
        }
        buffer.iterator
    }

    week_hbasePar.saveAsHadoopDataset(week_jobConf)


    sparkSession.stop()
  }

}