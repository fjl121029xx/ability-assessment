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
                      createTime: Long,
                      subject: Int)

case class TS_AbilityAssessment(userId: Long,
                                total_station_grade: Double,
                                total_station_predict_score: String,
                                do_exercise_num: Long,
                                cumulative_time: Long,
                                do_exercise_day: Long,
                                subject: Int
                               )

case class Week_AbilityAssessment(userId: Long,
                                  week_grade: Double,
                                  week_predict_score: String,
                                  subject: Int
                                 )

object AbilityAssessment {

  def main(args: Array[String]): Unit = {


    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"
    val collection = "ztk_answer_card"

    val conf = new SparkConf()
      .setAppName("AbilityAssessment")
      //      .setMaster("local[3]")
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
    val zac_df = sparkSession.sql("select userId,corrects,paper.questions,times,createTime,subject from ztk_answer_card")

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
            ac.get(4).asInstanceOf[Long].longValue(), //createTime
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
    val ts_userCount_x = sc.longAccumulator("ts_userCount_x")
    val ts_userCount_g = sc.longAccumulator("ts_userCount_g")
    val ts_userCount_z = sc.longAccumulator("ts_userCount_z")

    val ts_questionCount_x = sc.longAccumulator("ts_questionCount_x")
    val ts_questionCount_g = sc.longAccumulator("ts_questionCount_g")
    val ts_questionCount_z = sc.longAccumulator("ts_questionCount_z")

    val week_userCount_x = sc.longAccumulator("userCount")
    val week_userCount_g = sc.longAccumulator("userCount")
    val week_userCount_z = sc.longAccumulator("userCount")


    val week_questionCount = sc.longAccumulator("questionCount")

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
          } else if (subject == 2) {
            ts_userCount_g.add(1)
            ts_questionCount_g.add(predictedScore(1).toLong)
          } else if (subject == 3) {
            ts_userCount_z.add(1)
            ts_questionCount_z.add(predictedScore(1).toLong)

          }

          arr += TS_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore(0), subject), //total_station_grade: Double,
            predictedScore(0), //total_station_predict_score
            predictedScore(1).toLong, //do_exercise_num
            predictedScore(2).toLong, //cumulative_time
            predictedScore(4).toLong, //do_exercise_day
            subject
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
      "Row_Number() OVER(partition by subject order by total_station_predict_score desc) rank " +
      "from ts_predicted_score_df order by total_station_predict_score desc")

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
          } else if (subject == 2) {
            week_userCount_g.add(1)
          } else if (subject == 3) {
            week_userCount_z.add(1)
          }

          arr += Week_AbilityAssessment(
            userId, //userId
            PredictedScore.getScore(predictedScore(3), subject), //week_grade
            predictedScore(3), // week_predict_score
            subject
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
      "Row_Number() OVER(partition by subject order by week_predict_score desc) rank " +
      "from week_predicted_score_df  order by week_predict_score desc")
    week.show(5000)


    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.27,192.168.100.28,192.168.100.29")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.rootdir", "/hbase")
    hbaseConf.set("hbase.client.retries.number", "3")
    hbaseConf.set("hbase.rpc.timeout", "2000")
    hbaseConf.set("hbase.client.operation.timeout", "30")
    hbaseConf.set("hbase.client.scanner.timeout.period", "100")

    val total_station_jobConf = new JobConf(hbaseConf)
    total_station_jobConf.setOutputFormat(classOf[TableOutputFormat])
    total_station_jobConf.set(TableOutputFormat.OUTPUT_TABLE, "total_station_ability_assessment")
    val ts_hbasePar = ts.rdd.mapPartitions {
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


          val put = new Put(Bytes.toBytes(userId)) //行健的值
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("total_station_grade"), Bytes.toBytes(total_station_grade))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("total_station_predict_score"), Bytes.toBytes(total_station_predict_score))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("do_exercise_num"), Bytes.toBytes(do_exercise_num))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("cumulative_time"), Bytes.toBytes(cumulative_time))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("do_exercise_day"), Bytes.toBytes(do_exercise_day))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank))

          buffer += new Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }
    ts_hbasePar.saveAsHadoopDataset(total_station_jobConf)

    val week_jobConf = new JobConf(hbaseConf)
    week_jobConf.setOutputFormat(classOf[TableOutputFormat])
    week_jobConf.set(TableOutputFormat.OUTPUT_TABLE, "week_ability_assessment")
    val week_hbasePar = week.rdd.mapPartitions {
      ite: Iterator[Row] =>

        //          var lis: Seq[] = Seq()
        var buffer = new ArrayBuffer[Tuple2[ImmutableBytesWritable, Put]]()

        while (ite.hasNext) {
          val t = ite.next()

          val userId = t.get(0).asInstanceOf[Long].longValue()
          val week_grade = t.get(2).asInstanceOf[String].toString
          val week_predict_score = t.get(1).asInstanceOf[Double].doubleValue()
          val subject = t.get(3).asInstanceOf[Int].intValue()
          val rank = t.get(4).asInstanceOf[Int].intValue()


          val put = new Put(Bytes.toBytes(userId+"-"+TimeUtils.convertTimeStamp2DateStr(System.currentTimeMillis(),"yyyy-w"))) //行健的值
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_grade"), Bytes.toBytes(week_grade))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("week_predict_score"), Bytes.toBytes(week_predict_score))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("subject"), Bytes.toBytes(subject))
          put.add(Bytes.toBytes("ability_assessment_info"), Bytes.toBytes("rank"), Bytes.toBytes(rank))

          buffer += new Tuple2(new ImmutableBytesWritable, put)
          //            lis =  +: lis
        }
        buffer.iterator
    }

    week_hbasePar.saveAsHadoopDataset(total_station_jobConf)


  }

}