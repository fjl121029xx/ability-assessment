package com.li.ability.assessment


import com.li.ability.assessment.udaf.PredictedScore
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

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
    sc.setCheckpointDir("hdfs://huatu68/huatu/ability-assessment/checkpoint_dir/".concat(args(0)))
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
    val zac_df = sparkSession.sql("select userId,corrects,paper.questions,times,createTime from ztk_answer_card")

    zac_df.show(2000)
    zac_df.checkpoint()
    //    zac_df.write.save("ability-assessment/result_b/")
    //    zac_df.write.save(args(1))
    //    zac_df.rdd.saveAsObjectFile(args(0))
    val card = zac_df
      //      .limit(1000)
      .repartition(1000)
      .rdd.filter(f =>
      !f.isNullAt(0) && !f.isNullAt(1) && !f.isNullAt(2) && f.getSeq(2).nonEmpty && !f.isNullAt(3) && !f.isNullAt(4)
    )
      .mapPartitions { rite =>
        var arr = new ArrayBuffer[AnswerCard]()
        val q2pMap = q2p.value
        println(q2pMap.size)
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
          println(ac)
          var answerCard = AnswerCard(ac.get(0).asInstanceOf[Long].longValue(), ac.getSeq[Int](1), questions, ac.getSeq[Int](3), points, ac.get(4).asInstanceOf[Long].longValue())
          arr += answerCard
        }
        arr.iterator
      }
      .toDF()
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
      *
      * 482:0:46:1_
      * 754:4:45:39|
      * do_exercise_num 401|
      * cumulative_time 179|
      * week_predict_score -1:0:0:0]
      */
    predicted_score.show(1000)
    //    predicted_score.rdd.saveAsTextFile("ability-assessment/result_a/")
    predicted_score.rdd.saveAsTextFile("hdfs://huatu68/huatu/ability-assessment/result/".concat(args(0)))
    //      .rdd.saveAsTextFile(args(0))
  }

}