package com.li.ability.assessment


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
                                total_correct_num: Long
                               )

case class Week_AbilityAssessment(userId: Long,
                                  week_grade: Double,
                                  week_predict_score: String,
                                  subject: Int,
                                  week_do_exercise_num: Long,
                                  week_cumulative_time: Long,
                                  week_correct_num: Long
                                 )

object AbilityAssessment {

  def main(args: Array[String]): Unit = {


    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"
    val collection = "ztk_answer_card"

    val conf = new SparkConf()
      .setAppName("AbilityAssessment")
      //                  .setMaster("local[3]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .set("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
      .set("spark.mongodb.input.partitionKey", "_id")
      .set("spark.mongodb.input.partitionSizeMB", "5120")
      .set("spark.mongodb.input.samplesPerPartition", "5000000")
      .set("spark.debug.maxToStringFields", "100")
      .registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]], classOf[AnswerCard]))

    import com.mongodb.spark.sql._
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    // ztk_question
    /**
      * 获得题到知识点的映射
      */
    val ztk_question = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_question"),
          "maxBatchSize" -> "1000000",
          "keep_alive_ms" -> "500")
      )).toDF() // Uses the ReadConfig
    ztk_question.printSchema()
    ztk_question.createOrReplaceTempView("ztk_question")
    //    val q2p = sc.broadcast(map.collectAsMap())
    /**
      * mongo 214024
      * spark 205846
      * the mapping of the knowledge to points
      */
    // spark context
    val sc = sparkSession.sparkContext
    //    sc.setCheckpointDir("hdfs://huatu68/huatu/ability-assessment/checkpoint_dir/".concat(args(0)))
    val q2p = sc.broadcast(sparkSession.sql("select _id,points from ztk_question").rdd.filter { r =>
      var flag = true
      flag = !r.isNullAt(0) && !r.isNullAt(1) && r.getSeq(1).nonEmpty
      //      if (flag) {
      //        flag = r.get(0).getClass.getName match {
      //          case "java.lang.Double" => false
      //          case _ => true
      //        }
      //      }
      flag
    }.map {
      r =>
        val _id: Int = r.get(0).asInstanceOf[Number].intValue()
        val pid = r.getSeq(1).head.asInstanceOf[Double].intValue()
        (_id, pid)
    }.collectAsMap())
    //    Thread.sleep(5000)
    println(q2p.value.size)
    if (q2p.value.isEmpty) {
      sparkSession.stop()
    }

    // ztk_answer_card
    val ztk_answer_card = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_answer_card"),
          "partitionSizeMB" -> "5120",
          "maxBatchSize" -> "1000000",
          "samplesPerPartition" -> "5000000"
        )
      )).toDF() // Uses the ReadConfig
    ztk_answer_card.createOrReplaceTempView("ztk_answer_card")
    ztk_answer_card.printSchema()
    val zac_df = sparkSession.sql("select userId,corrects,paper.questions,times,createTime,subject from ztk" +
      "_answer_card")
    //        .limit(100)

    zac_df.show(2000)
    //    zac_df.checkpoint()
    //    zac_df.write.save("ability-assessment/result_b/")
    //    zac_df.write.save(args(1))
    //    zac_df.rdd.saveAsObjectFile(args(0))
    val card = zac_df
      //            .limit(1000)
      .repartition(1000)
      .rdd.filter(f =>
      !f.isNullAt(0) && !f.isNullAt(1) && !f.isNullAt(2) && f.getSeq(2).nonEmpty && !f.isNullAt(3) && !f.isNullAt(4) && !f.isNullAt(5)
    )
      .mapPartitions { rite =>
        var arr = new ArrayBuffer[AnswerCard]()
        val q2pMap = q2p.value

        while (rite.hasNext) {
          // answer card row
          val ac = rite.next()
          // qid to pid
          // qids
          val questions: Seq[Int] = ac.getSeq(2)
          //
          val points = new ArrayBuffer[Int]()
          questions.foreach { qid =>
            //            println(qid)
            //            println(q2pMap.getOrElse(qid, 0))
            //            if (qid == 55309) {
            //              println(qid + "___" + q2pMap.get(qid))
            //            }
            val pid: Int = q2pMap.getOrElse(qid, 0)
            points += pid
          }

          arr += AnswerCard(
            ac.get(0).asInstanceOf[Long].longValue(), //userId
            ac.getSeq[Int](1), //corrects
            questions, //questions
            ac.getSeq[Int](3), //times
            points, //points
            ac.get(4).asInstanceOf[Long].toString, //createTime
            ac.get(5).asInstanceOf[Int].intValue()) //subject
        }
        arr.iterator
      }
      .toDF()
    card.show(1000)

    // val total_station = mongo.select("userId", "subject", "catgory", "expendTime", "createTime", "corrects", "paper.questions", "paper.modules", "StringCount")
    card.createOrReplaceTempView("answer_card")
    sparkSession.udf.register("predictedScore", new PredictedScore)
    val predicted_score = sparkSession.sql("select userId,predictedScore(corrects,questions,times,points,createTime) predictedScore,subject from answer_card group by userId,subject")

    predicted_score.show(3000)
    //    predicted_score.rdd.saveAsTextFile("ability-assessment/result_a/")
    // 累加器
    /**
      * 全站
      */
    // 统计科目下的用户数量
    val ts_userCount_x = sc.longAccumulator("ts_userCount_x")
    val ts_userCount_g = sc.longAccumulator("ts_userCount_g")
    val ts_userCount_z = sc.longAccumulator("ts_userCount_z")
    // 统计科目下的做题数量
    val ts_questionCount_x = sc.longAccumulator("ts_questionCount_x")
    val ts_questionCount_g = sc.longAccumulator("ts_questionCount_g")
    val ts_questionCount_z = sc.longAccumulator("ts_questionCount_z")
    // 统计科目下的做题时长
    val ts_cumulative_time_x = sc.longAccumulator("ts_cumulative_time_x")
    val ts_cumulative_time_g = sc.longAccumulator("ts_cumulative_time_g")
    val ts_cumulative_time_z = sc.longAccumulator("ts_cumulative_time_z")
    // 统计科目下的正确数量
    val ts_correct_num_x = sc.longAccumulator("ts_correct_num_x")
    val ts_correct_num_g = sc.longAccumulator("ts_correct_num_g")
    val ts_correct_num_z = sc.longAccumulator("ts_correct_num_z")
    /**
      * 周
      */
    // 周:统计科目下的用户数量
    val week_userCount_x = sc.longAccumulator("week_userCount_x")
    val week_userCount_g = sc.longAccumulator("week_userCount_g")
    val week_userCount_z = sc.longAccumulator("week_userCount_z")

    // 周:统计科目下的做题数量
    val week_questionCount_x = sc.longAccumulator("week_questionCount_x")
    val week_questionCount_g = sc.longAccumulator("week_questionCount_g")
    val week_questionCount_z = sc.longAccumulator("week_questionCount_z")
    // 周:统计科目下的做题时长
    val week_cumulative_time_x = sc.longAccumulator("week_cumulative_time_x")
    val week_cumulative_time_g = sc.longAccumulator("week_cumulative_time_g")
    val week_cumulative_time_z = sc.longAccumulator("week_cumulative_time_z")
    // 周:统计科目下的正确数量
    val week_correct_num_x = sc.longAccumulator("week_correct_num_x")
    val week_correct_num_g = sc.longAccumulator("week_correct_num_g")
    val week_correct_num_z = sc.longAccumulator("week_correct_num_z")

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
            ts_userCount_x.add(1)
            ts_questionCount_x.add(predictedScore(1).toLong)
            ts_cumulative_time_x.add(predictedScore(2).toLong)
            ts_correct_num_x.add(predictedScore(7).toLong)
          } else if (subject == 2) {
            ts_userCount_g.add(1)
            ts_questionCount_g.add(predictedScore(1).toLong)
            ts_cumulative_time_g.add(predictedScore(2).toLong)
            ts_correct_num_g.add(predictedScore(7).toLong)
          } else if (subject == 3) {
            ts_userCount_z.add(1)
            ts_questionCount_z.add(predictedScore(1).toLong)
            ts_cumulative_time_z.add(predictedScore(2).toLong)
            ts_correct_num_z.add(predictedScore(7).toLong)
          }

          arr += TS_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore(0), subject), //total_station_grade: Double,
            predictedScore(0), //total_station_predict_score
            predictedScore(1).toLong, //do_exercise_num
            predictedScore(2).toLong, //cumulative_time
            predictedScore(4).toLong, //do_exercise_day
            subject,
            predictedScore(7).toLong  //total_correct_num
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
      "Row_Number() OVER(partition by subject order by total_station_grade desc) rank " +
      "from ts_predicted_score_df")

    ts.show(5000)

    val week_predicted_score_df = predicted_score_rdd.mapPartitions {
      ite =>
        var arr = new ArrayBuffer[Week_AbilityAssessment]()
        while (ite.hasNext) {
          val n = ite.next()
          val userId = n.get(0).asInstanceOf[Long].longValue()
          val predictedScore = n.get(1).asInstanceOf[Seq[String]].seq
          val subject = n.get(2).asInstanceOf[Int].intValue()

          if (subject == 1) {
            week_userCount_x.add(1)
            week_questionCount_x.add(predictedScore(1).toLong)
            week_cumulative_time_x.add(predictedScore(2).toLong)
            week_correct_num_x.add(predictedScore(8).toLong)
          } else if (subject == 2) {
            week_userCount_g.add(1)
            week_questionCount_g.add(predictedScore(1).toLong)
            week_cumulative_time_g.add(predictedScore(2).toLong)
            week_correct_num_g.add(predictedScore(8).toLong)
          } else if (subject == 3) {
            week_userCount_z.add(1)
            week_questionCount_z.add(predictedScore(1).toLong)
            week_cumulative_time_z.add(predictedScore(2).toLong)
            week_correct_num_z.add(predictedScore(8).toLong)
          }


          arr += Week_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore(3), subject), //week_grade
            predictedScore(3), // week_predict_score
            subject,
            predictedScore(5).toLong,
            predictedScore(6).toLong,
            predictedScore(8).toLong
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
      "week_cumulative_time " +
      "from week_predicted_score_df  ")
    week.show(5000)


    val week_unum_x = sc.broadcast(week_userCount_x.value.toString)
    val week_qnum_x = sc.broadcast(week_questionCount_x.value.toString)
    val week_tnum_x = sc.broadcast(week_cumulative_time_x.value.toString)

    val week_unum_g = sc.broadcast(week_userCount_g.value.toString)
    val week_qnum_g = sc.broadcast(week_questionCount_g.value.toString)
    val week_tnum_g = sc.broadcast(week_cumulative_time_g.value.toString)

    val week_unum_z = sc.broadcast(week_userCount_g.value.toString)
    val week_qnum_z = sc.broadcast(week_questionCount_g.value.toString)
    val week_tnum_z = sc.broadcast(week_cumulative_time_g.value.toString)

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


          val week_grade = t.get(2).asInstanceOf[String].toString
          val week_predict_score = t.get(1).asInstanceOf[Double].doubleValue()
          val subject = t.get(3).asInstanceOf[Int].intValue()
          val rank = t.get(4).asInstanceOf[Int].intValue()
          val week_do_exercise_num = t.get(5).asInstanceOf[Long].longValue()
          val week_cumulative_time = t.get(6).asInstanceOf[Long].longValue()

          val put = new Put(Bytes.toBytes(userId + "-" + TimeUtils.convertTimeStamp2DateStr(System.currentTimeMillis(), "yyyy-w"))) //行健的值
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_grade"), Bytes.toBytes(week_grade.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_predict_score"), Bytes.toBytes(week_predict_score.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_do_exercise_num"), Bytes.toBytes(week_do_exercise_num.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_cumulative_time"), Bytes.toBytes(week_cumulative_time.toString))


          if (subject == 1) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_userCount"), Bytes.toBytes(week_unum_x.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_questionCount"), Bytes.toBytes(week_qnum_x.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_cumulative_timeTotal"), Bytes.toBytes(week_tnum_x.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_correct_num"), Bytes.toBytes(week_correct_num_x.value))
          } else if (subject == 2) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_userCount"), Bytes.toBytes(week_unum_g.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_questionCount"), Bytes.toBytes(week_qnum_g.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_cumulative_timeTotal"), Bytes.toBytes(week_tnum_g.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_correct_num"), Bytes.toBytes(week_correct_num_g.value))
          } else if (subject == 3) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_userCount"), Bytes.toBytes(week_unum_z.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_questionCount"), Bytes.toBytes(week_qnum_z.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_cumulative_timeTotal"), Bytes.toBytes(week_tnum_z.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_correct_num"), Bytes.toBytes(week_correct_num_z.value))
          }

          buffer += new Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }

    week_hbasePar.saveAsHadoopDataset(week_jobConf)


    val ts_unum_x = sc.broadcast(ts_userCount_x.value.toString)
    val ts_qnum_x = sc.broadcast(ts_questionCount_x.value.toString)
    val ts_tnum_x = sc.broadcast(ts_cumulative_time_x.value.toString)

    val ts_unum_g = sc.broadcast(ts_userCount_g.value.toString)
    val ts_qnum_g = sc.broadcast(ts_questionCount_g.value.toString)
    val ts_tnum_g = sc.broadcast(ts_cumulative_time_g.value.toString)

    val ts_unum_z = sc.broadcast(ts_userCount_z.value.toString)
    val ts_qnum_z = sc.broadcast(ts_questionCount_z.value.toString)
    val ts_tnum_z = sc.broadcast(ts_cumulative_time_z.value.toString)

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

        //          var lis: Seq[] = Seq()
        var buffer = new ArrayBuffer[Tuple2[ImmutableBytesWritable, Put]]()

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


          val put = new Put(Bytes.toBytes(userId.toString + "-" + subject)) //行健的值
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("total_station_grade"), Bytes.toBytes(total_station_grade.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("total_station_predict_score"), Bytes.toBytes(total_station_predict_score.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("do_exercise_num"), Bytes.toBytes(do_exercise_num.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("cumulative_time"), Bytes.toBytes(cumulative_time.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("do_exercise_day"), Bytes.toBytes(do_exercise_day.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject.toString))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank.toString))


          if (subject == 1) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("ts_userCount"), Bytes.toBytes(ts_unum_x.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("ts_questionCount"), Bytes.toBytes(ts_qnum_x.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("ts_cumulative_time"), Bytes.toBytes(ts_tnum_x.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("total_correct_num"), Bytes.toBytes(ts_correct_num_x.value))
          } else if (subject == 2) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("ts_userCount"), Bytes.toBytes(ts_unum_g.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("ts_questionCount"), Bytes.toBytes(ts_qnum_g.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("total_correct_num"), Bytes.toBytes(ts_correct_num_g.value))
          } else if (subject == 3) {
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("ts_userCount"), Bytes.toBytes(ts_unum_z.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("ts_questionCount"), Bytes.toBytes(ts_qnum_z.value))
            put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("total_correct_num"), Bytes.toBytes(ts_correct_num_z.value))
          }

          buffer += new Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }
    ts_hbasePar.saveAsHadoopDataset(total_station_jobConf)
    sparkSession.stop()
  }

}