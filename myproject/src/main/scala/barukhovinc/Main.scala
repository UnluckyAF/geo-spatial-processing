package barukhovinc

import geotrellis.layer._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark.store.hadoop._
import org.apache.hadoop.fs.Path
import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.resample._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.layer.{SpatialKey, SpaceTimeKey, FloatingLayoutScheme}
import geotrellis.spark.store._
import geotrellis.spark.tiling.Tiler
import geotrellis.vector._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import cats.implicits._
import com.monovore.decline._

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._

import org.log4s._

import scala.collection.immutable.ArraySeq

object  Main {
  @transient private[this] lazy val logger = getLogger

  def main(args: Array[String]): Unit = {
    Console.println(scala.util.Properties.versionString)
    // val input: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_B1.TIF"
    // val output: String = "/Users/barukhov/geo_spatial_data/res"
    // val opName: String = "crop"
    // val otherArgs: Array[String] = Array[String]("300000.000", "6000000.000", "500000.000", "6100000.000")
    // val input1: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_B1.TIF"
    // val input2: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_QA_PIXEL.TIF"
    // val output: String = "/Users/barukhov/geo_spatial_data/res"
    // val opName: String = "add"
    val otherArgs: Array[String] = Array[String]("1")
    val input1: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_B1.TIF"
    val output: String = "/Users/barukhov/geo_spatial_data/res"
    val opName: String = "focalSum"
    run(List[String](input1), output, opName, otherArgs)(Spark.context)
    Spark.session.stop()
  }

  def read(inputs: List[String])(implicit sc: SparkContext): List[TileLayerRDD[SpatialKey]] = {
    var tileInputs: List[TileLayerRDD[SpatialKey]] = List[TileLayerRDD[SpatialKey]]()

    for (path <- inputs) {
      val pth = new Path(path)
      Console.println(pth)
      val inputRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(pth)

      val layoutScheme = FloatingLayoutScheme(1024)

      val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
        inputRdd.collectMetadata[SpatialKey](layoutScheme)

      val tilerOptions =
        Tiler.Options(
          resampleMethod = Average,
          // partitioner = new RoundRobin(inputRdd.partitions.length)
          partitioner = new HashPartitioner(inputRdd.partitions.length)
          // partitioner = new RoundRobin(inputRdd.partitions.length)
          // https://stackoverflow.com/questions/23127329/how-to-define-custom-partitioner-for-spark-rdds-of-equally-sized-partition-where
        )

      val tiledRdd =
        inputRdd.tileToLayout[SpatialKey](metadata, tilerOptions)


      // At this point, we want to combine our RDD and our Metadata to get a TileLayerRDD[SpatialKey]

      val layerRdd: TileLayerRDD[SpatialKey] =
        ContextRDD(tiledRdd, metadata)

      tileInputs = layerRdd :: tileInputs
      Console.println(inputRdd.partitions.length)
    }

    return tileInputs

  }

  def write(res: TileLayerRDD[SpatialKey], output: String): Unit = {
    val stitched = res.stitch()

    val geotiff = GeoTiff(stitched, res.metadata.crs)
    val pathHadoop = new Path(output + "_spark")
    geotiff.write(pathHadoop, res.sparkContext.hadoopConfiguration)
  }

  def crop(tileInputs: List[TileLayerRDD[SpatialKey]], boundingBox: ArraySeq[Double]): TileLayerRDD[SpatialKey] = {
    val currentRDD: TileLayerRDD[SpatialKey] = tileInputs.head
    val minX: Double = boundingBox(0)
    val minY: Double = boundingBox(1)
    val maxX: Double = boundingBox(2)
    val maxY: Double = boundingBox(3)
    val areaOfInterest: Extent = Extent(minX, minY, maxX, maxY)

    val cropedRDD = currentRDD.crop(areaOfInterest)
    return cropedRDD
  }

  def add(tileInputs: List[TileLayerRDD[SpatialKey]]): TileLayerRDD[SpatialKey] = {
    return ContextRDD(tileInputs.head + tileInputs.last, tileInputs.head.metadata)
  }

  def focalSum(tileInputs: List[TileLayerRDD[SpatialKey]], padding: Int): TileLayerRDD[SpatialKey] = {
    return ContextRDD(tileInputs.head.focalSum(Square(padding)), tileInputs.head.metadata)
  }

  def run(inputs: List[String], output: String, op: String, otherArgs: Array[String])(implicit sc: SparkContext): Unit = {
    val tileInputs: List[TileLayerRDD[SpatialKey]] = read(inputs)(sc)

    op match {
      case "crop" =>
        val res = crop(tileInputs, ArraySeq.unsafeWrapArray(otherArgs.map(_.toDouble)))
        write(res, output)
      case "add" =>
        val res = add(tileInputs)
        write(res, output)
      case "focalSum" =>
        val res = focalSum(tileInputs, otherArgs(0).toInt)
        write(res, output)
    }
// 300000.000, 6000000.000, 500000.000, 6100000.000

    // val addRes = layerRdd +

  }
}
