package com.li.ability.assessment


import com.li.ability.assessment.udaf.PredictedScore
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, Map}


case class AnswerCard(userId: Long,
                      corrects: Seq[Int],
                      questions: Seq[Int],
                      times: Seq[Int],
                      points: Seq[Int],
                      createTime: Long)

object AbilityAssessment {

  def main(args: Array[String]): Unit = {


    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"
    val collection = "ztk_answer_card"

    val conf = new SparkConf()
      .setAppName("AbilityAssessment")
      //      .setMaster("local[13]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.mongodb.input.uri", inputUrl)
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
      .set("spark.mongodb.input.partitionerOptions.partitionSizeMB", "1024")
      .set("spark.mongodb.keep_alive_ms", "3600000000000")
      .registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]], classOf[AnswerCard]))

    import com.mongodb.spark.sql._
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    import sparkSession.implicits._
    // spark context
    val sc = sparkSession.sparkContext

    // ztk_question
    /**
      * 获得题到知识点的映射
      */
    val ztk_question = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_question"),
          "readPreference.name" -> "secondaryPreferred",
          "partitionKey" -> "_id")
      )).toDF() // Uses the ReadConfig
    ztk_question.createOrReplaceTempView("ztk_question")
    //    val q2p = sc.broadcast(map.collectAsMap())
    /**
      * mongo 214024
      * spark 205846
      * the mapping of the knowledge to points
      */
    val q2p = sc.broadcast(sparkSession.sql("select _id,points from ztk_question").rdd.filter { r =>
      var flag = true

      flag = !r.isNullAt(0) && !r.isNullAt(1) && r.getSeq(1).nonEmpty
      if (flag) {
        flag = r.get(0).getClass.getName match {
          case "java.lang.Double" => false
          case _ => true
        }
      }
      flag
    }.map {
      r =>
        val _id: Int = r.getInt(0)
        val pid: Int = r.getSeq(1).head
        //        if (_id == 55309) {b
        //          println(_id + "___" + pid)
        //        }
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
          "readPreference.name" -> "secondaryPreferred",
          "partitionKey" -> "userId",
          "samplesPerPartition" -> "10000"
        )
      )).toDF() // Uses the ReadConfig
    ztk_answer_card.createOrReplaceTempView("ztk_answer_card")
    val card = sparkSession.sql("select userId,corrects,paper.questions,times,createTime from ztk_answer_card ")
      //      .limit(1000000)
      .repartition(1000)

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
            val pid: Int = q2pMap.get(qid).get
            points += pid
          }

          var answerCard = AnswerCard(ac.getLong(0), ac.getSeq(1), questions, ac.getSeq(3), points, ac.getLong(4))
          arr += answerCard
        }
        arr.iterator
      }.filter { ac => ac.userId.isValidLong && ac.corrects.nonEmpty && ac.points.nonEmpty && ac.corrects.nonEmpty && ac.times.nonEmpty }
      .toDF()
    card.cache()
    card.show(1000)

    // val total_station = mongo.select("userId", "subject", "catgory", "expendTime", "createTime", "corrects", "paper.questions", "paper.modules", "StringCount")
    card.createOrReplaceTempView("answer_card")
    sparkSession.udf.register("predictedScore", new PredictedScore)
    val predicted_score = sparkSession.sql("select userId,predictedScore(corrects,questions,times,points,createTime) predictedScore from answer_card group by userId")

    //    predicted_score.show(100000)
    /**
      * [
      * 1,
      * total_station_predict_score
      * -1:0:0:0_
      * 642:0:93:0_
      * 435:7:152:72_
      * 392:3:68:67_
      * 482:0:46:1_
      * 754:4:45:39|
      * do_exercise_num 401|
      * cumulative_time 179|
      * week_predict_score -1:0:0:0]
      */
    predicted_score.rdd.saveAsTextFile(args(0))
    //      .rdd.saveAsTextFile(args(0))
  }

}