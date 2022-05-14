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
    val input1: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_B4.TIF"
    val input2: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_B3.TIF"
    val input3: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_B5.TIF"
    val output: String = "/Users/barukhov/geo_spatial_data/res"
    val opName: String = "focalSum"
    run(List[String](input1, input2, input3), output, opName, otherArgs)(Spark.context)
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

  def oneRead(input: String)(implicit sc: SparkContext): (RDD[(SpatialKey,MultibandTile)], TileLayerMetadata[SpatialKey]) = {
    val pth = new Path(input)
    Console.println(pth)
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
    sc.hadoopMultibandGeoTiffRDD(pth)

    val layoutScheme = FloatingLayoutScheme(512)

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

    // val layerRdd: MultibandTileLayerRDD[SpatialKey] =
    //   ContextRDD(tiledRdd, metadata)

    Console.println(inputRdd.partitions.length)
    return (tiledRdd, metadata)
  }

  def multiRead(inputs: List[String])(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    // var tileInputs: List[MultibandTileLayerRDD[SpatialKey]] = List[MultibandTileLayerRDD[SpatialKey]]()
    var (resRDD: RDD[(SpatialKey,MultibandTile)], spec_metadata: TileLayerMetadata[SpatialKey]) = oneRead(inputs.head)(sc)

    val layoutScheme = FloatingLayoutScheme(512)

    for (path <- inputs.tail) {
      val (rdd: RDD[(SpatialKey,MultibandTile)], _: TileLayerMetadata[SpatialKey]) = oneRead(path)(sc)
      resRDD = resRDD.union(rdd)
    }

    // val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
    //   resRDD.collectMetadata[SpatialKey](layoutScheme)

    val layerRdd: MultibandTileLayerRDD[SpatialKey] =
      ContextRDD(resRDD, spec_metadata)
    // var res: MultibandTileLayerRDD[SpatialKey] = tileInputs.head
    // for (rdd <- tileInputs.tail) {
    //   res = res.union(rdd)
    // }
    return layerRdd
  }

  def removeClouds(layerRdd: MultibandTileLayerRDD[SpatialKey])(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val input4: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_QA_AEROSOL.TIF"
    val pth = new Path(input4)
    val qaResRDD: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(pth)
    // var (qaResRDD: RDD[(SpatialKey,MultibandTile)], _: TileLayerMetadata[SpatialKey]) = oneRead(input4)(sc)
    // val qaLayerRdd: MultibandTileLayerRDD[SpatialKey] =
    //   ContextRDD(qaResRDD, qa_spec_metadata)
    def maskClouds(rdd: MultibandTileLayerRDD[SpatialKey]): MultibandTileLayerRDD[SpatialKey] = {
      val res = rdd.withContext{
        rd => rd.mapValues{
          x => x.mapBands{
            (_, y) => y.combine(qaResRDD.first._2.tile) {(v: Int, qa: Int) =>
              val isCloud = qa & 0x8000
              val isCirrus = qa & 0x2000
              if(isCloud > 0 || isCirrus > 0) { NODATA }
              else { v }
            }
          }
        }
      }
      return res
    }
      // tile.combine(qaLayerRdd) { (v: Int, qa: Int) =>
      //   val isCloud = qa & 0x8000
      //   val isCirrus = qa & 0x2000
      //   if(isCloud > 0 || isCirrus > 0) { NODATA }
      //   else { v }
      // }

    val masked = maskClouds(layerRdd)
    val res = masked.convert(IntConstantNoDataCellType)

    return res
  }

  def write(res: TileLayerRDD[SpatialKey], output: String): Unit = {
    val stitched = res.stitch()

    val geotiff = GeoTiff(stitched, res.metadata.crs)
    val pathHadoop = new Path(output + "_spark")
    geotiff.write(pathHadoop, res.sparkContext.hadoopConfiguration)
  }

  def multiWrite(res: MultibandTileLayerRDD[SpatialKey], output: String): Unit = {
    val stitched = res.stitch()

    val geotiff = GeoTiff(stitched, res.metadata.crs)
    geotiff.write(output + "_spark")
    // val pathHadoop = new Path(output + "_spark")
    // geotiff.write(pathHadoop, res.sparkContext.hadoopConfiguration)
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
    op match {
      case "crop" =>
        val tileInputs: List[TileLayerRDD[SpatialKey]] = read(inputs)(sc)
        val res = crop(tileInputs, ArraySeq.unsafeWrapArray(otherArgs.map(_.toDouble)))
        write(res, output)
      case "add" =>
        val tileInputs: List[TileLayerRDD[SpatialKey]] = read(inputs)(sc)
        val res = add(tileInputs)
        write(res, output)
      case "focalSum" =>
        val tileInputs: List[TileLayerRDD[SpatialKey]] = read(inputs)(sc)
        val res = focalSum(tileInputs, otherArgs(0).toInt)
        write(res, output)
      case "test" =>
        val tileInputs: MultibandTileLayerRDD[SpatialKey] = multiRead(inputs)(sc)
        val res = removeClouds(tileInputs)
        multiWrite(res, output)
    }
// 300000.000, 6000000.000, 500000.000, 6100000.000

    // val addRes = layerRdd +

  }
}
