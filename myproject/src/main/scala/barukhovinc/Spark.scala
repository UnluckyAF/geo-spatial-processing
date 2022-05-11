package barukhovinc

import geotrellis.layer.SpatialKey

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.Properties

object Spark {
  def conf: SparkConf = new SparkConf()
    // .setIfMissing("spark.master", "local[*]")
    // .setIfMissing("spark.master", "local-cluster[1,2,7168]")
    .setAppName("MyProject")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    .set("spark.kryoserializer.buffer.max", "1g")
    // .setExecutorEnv("SPARK_HOME", Properties.envOrElse("SPARK_HOME", "/usr/local/spark-3.1.3"))
    // .setSparkHome(Properties.envOrElse("SPARK_HOME", "/usr/local/spark-3.1.3"))
    // .setExecutorEnv("SPARK_SCALA_VERSION", Properties.envOrElse("SPARK_SCALA_VERSION", "2.12"))
    // .setExecutorEnv("JAVA_HOME", Properties.envOrElse("JAVA_HOME", "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"))
    // .setExecutorEnv("GEOTRELLIS_HOME", Properties.envOrElse("GEOTRELLIS_HOME", "/usr/local/geotrellis"))
    // .setJars(Array[String](
    //   "/usr/local/geotrellis/geotrellis-spark_2.12-3.6.0-SNAPSHOT.jar",
    //   "/usr/local/geotrellis/geotrellis-s3_2.12-3.6.0-SNAPSHOT.jar",
    //   "/usr/local/geotrellis/geotrellis-gdal_2.12-3.6.0-SNAPSHOT.jar",
    // ))
    // .setJars(Array[String](
    //   "/usr/local/spark/jars/hadoop-client-api-3.3.1.jar",
    //   "/usr/local/spark/jars/hadoop-client-runtime-3.3.1.jar",
    //   "/usr/local/spark/jars/hadoop-shaded-guava-1.1.1.jar",
    //   "/usr/local/spark/jars/hadoop-yarn-server-web-proxy-3.3.1.jar",
    //   "/usr/local/spark/jars/guava-14.0.1.jar",
    //   "/usr/local/spark/jars/htrace-core4-4.1.0-incubating.jar",
    // ))

  implicit val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
  implicit def context: SparkContext = session.sparkContext

  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.map(_._1)
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
  }
}

class SamePartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    Console.println("DEBUG", key)
    return 0
  }
}

class RoundRobin(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  var last_partition: Int = -1

  override def getPartition(key: Any): Int = {
    last_partition = (last_partition + 1) % numPartitions
    Console.println("DEBUG", key, last_partition)
    return last_partition
  }
}

