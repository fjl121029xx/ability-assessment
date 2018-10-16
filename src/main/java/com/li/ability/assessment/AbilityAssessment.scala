package com.li.ability.assessment

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import com.mongodb.spark.config._

import scala.collection.mutable.ArrayBuffer


object AbilityAssessment {

  def main(args: Array[String]): Unit = {

    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"
    val collection = "ztk_answer_card"

    val conf = new SparkConf()
      .setAppName("AbilityAssessment")
      .setMaster("local[13]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sql = SparkSession.builder
      .config(conf)
      .config("spark.mongodb.input.uri", inputUrl)
      .config("spark.mongodb.input.collection", collection)
      .config("spark.debug.maxToStringFields", 100)
      .getOrCreate()

    import sql.implicits._

    val mongo = sql.read.format("com.mongodb.spark.sql").options(
      Map("spark.mongodb.input.uri" -> inputUrl,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey" -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB" -> "1024",
        "spark.mongodb.keep_alive_ms" -> "3600000000000"
      )
    ).load


    val answer_card_df = mongo.select( "userId", "subject", "catgory", "expendTime", "createTime", "corrects", "paper.questions", "paper.modules","times")
      .rdd
      .map(t => (t.getLong(0), t.getInt(1), t.getInt(2), t.getInt(3), t.getLong(4), t.getSeq(5), t.getSeq(6), t.getSeq(7), t.getSeq(8)))

    answer_card_df.cache()


    answer_card_df.flatMap(f=>{
      val arr = new ArrayBuffer[Tuple10[]]()

      arr.iterator
    })
    answer_card_df.take(10000).foreach(println)

    //    answer_card_df.take(1).foreach(println)
    //   val answer_card_rdd = answer_card_df.rdd.repartition(1)
    //    answer_card_df.rdd.saveAsTextFile("hdfs://master/ztk_question_record/ztk_answer_card/")
    //    answer_card_df.write.json("hdfs://master/ztk_question_record/ztk_answer_card/")
  }

}
