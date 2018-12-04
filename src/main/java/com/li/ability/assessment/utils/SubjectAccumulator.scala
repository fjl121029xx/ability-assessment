package com.li.ability.assessment.utils

import org.apache.spark.util.AccumulatorV2

/**
  *
  */
class SubjectAccumulator extends AccumulatorV2[String, String] {
  /**
    * "ucount=0,qcount=0,timeTotal=0,cnum=0"
    */
  private var res = "0,0,0,0"

  override def isZero: Boolean = res == "" || res == "0,0,0,0"

  override def copy(): AccumulatorV2[String, String] = {

    val myAcc = new SubjectAccumulator
    myAcc.res = this.res

    myAcc
  }

  override def reset(): Unit = res = "0,0,0,0"

  override def add(v: String): Unit = abc(res, v)

  override def merge(other: AccumulatorV2[String, String]): Unit = other match {

    case o: SubjectAccumulator => abc(res, o.res)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: String = res

  private def abc(res1: String, res2: String): String = {

    var tmp = ""
    val init = res1.split(",").map(_.toLong).toSeq
    val toadd = res2.split(",").map(_.toLong).toSeq


    tmp = tmp.concat(init(0) + toadd(0) + "").concat(",")
      .concat(init(1) + toadd(1) + "").concat(",")
      .concat(init(2) + toadd(2) + "").concat(",")
      .concat(init(3) + toadd(3) + "")

    //    for (i <- init; j <- toadd) {
    //      tmp.concat(i + j + "").concat(",")
    //    }
    tmp = tmp.substring(0, tmp.length - 1)
    tmp
  }
}
