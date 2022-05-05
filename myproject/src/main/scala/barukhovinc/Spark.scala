package barukhovinc

import geotrellis.layer.SpatialKey

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.Properties

object Spark {
  def conf: SparkConf = new SparkConf()
    .setIfMissing("spark.master", "local[*]")
    .setAppName("MyProject")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    // .set("spark.executionEnv.AWS_PROFILE", Properties.envOrElse("AWS_PROFILE", "default"))

  implicit val session: SparkSession = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
  implicit def context: SparkContext = session.sparkContext
}

class SamePartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    Console.println("DEBUG", key)
    return 0
  }
}
