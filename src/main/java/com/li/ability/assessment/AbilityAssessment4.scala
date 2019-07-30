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

case class S_C_A_A(userId: Long,
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

    System.setProperty("HADOOP_USER_NAME", "root")
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val conf = new SparkConf()
      .setAppName("shence-data")
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

    options += ("dbtable" -> "v_qbank_user")
    val v_qbank_user = sparkSession.read.format("jdbc").options(options).load
    v_qbank_user.createOrReplaceTempView("v_qbank_user")


    // v_new_subject
    options += ("dbtable" -> "knowledge_subject")
    val v_new_subject = sparkSession.read.format("jdbc").options(options).load
    v_new_subject.createOrReplaceTempView("knowledge_subject")

    val b_user_phone = sc.broadcast(sparkSession.sql("select PUKEY,reg_phone from v_qbank_user")
      .mapPartitions(
        ite => {
          val arr = ArrayBuffer[(Long, String)]()

          while (ite.hasNext) {
            val l = ite.next()

            val user_id = l.getLong(0)
            val user_phone = l.getString(1)
            arr += Tuple2(user_id, user_phone)
          }

          arr.iterator
        }
      ).rdd.collectAsMap())

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


    predicted_score.cache()

    val ts_predicted_score_df = predicted_score.mapPartitions {
      ite =>
        var arr = new ArrayBuffer[S_C_A_A]()
        val m = subjetPointName.value

        val b_v_user_phone = b_user_phone.value

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
      "subject  " +
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
          val sortScore = t.get(11).asInstanceOf[Double].doubleValue()


          val put = new Put(Bytes.toBytes(userId.toString + "-" + subject)) //行健的值
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("grade"), Bytes.toBytes(total_station_grade.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("predictScore"), Bytes.toBytes(total_station_predict_score.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseNum"), Bytes.toBytes(do_exercise_num.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseTime"), Bytes.toBytes(cumulative_time.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("exerciseDay"), Bytes.toBytes(do_exercise_day.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.addColumn(Bytes.toBytes(t_family), Bytes.toBytes("sortScore"), Bytes.toBytes(sortScore.toString))


          buffer += Tuple2(new ImmutableBytesWritable, put)
        }
        buffer.iterator
    }
    ts_hbasePar.saveAsHadoopDataset(total_station_jobConf)


    sparkSession.stop()
  }

}