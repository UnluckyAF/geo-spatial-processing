package barukhovinc

import geotrellis.layer._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark.store.hadoop._
import org.apache.hadoop.fs.Path
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

  // private val inputsOpt = Opts.options[String](
  //   "inputs", help = "The path that points to data that will be read")
  // private val outputOpt = Opts.option[String](
  //   "output", help = "The path of the output tiffs")
  // private val partitionsOpt =  Opts.option[Int](
  //   "numPartitions", help = "The number of partitions to use").orNone

  // private val command = Command(name = "MyProject", header = "GeoTrellis App: MyProject") {
  //   (inputsOpt, outputOpt, partitionsOpt).tupled
  // }

  def main(args: Array[String]): Unit = {
    // command.parse(ArraySeq.unsafeWrapArray(args), sys.env) match {
    //   case Left(help) =>
    //     System.err.println(help)
    //     sys.exit(1)

    //   case Right((i, o, p)) =>
    //     try {
    //       run(i.toList, o, p)(Spark.context)
    //     } finally {
    //       Spark.session.stop()
    //     }
    // }
    Console.println(scala.util.Properties.versionString)
    val input: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_B1.TIF"
    val output: String = "/Users/barukhov/geo_spatial_data/res"
    run(List[String]{input}, output, Option[Int]{0})(Spark.context)
    Spark.session.stop()
  }

  def run(inputs: List[String], output: String, numPartitions: Option[Int])(implicit sc: SparkContext): Unit = {
    val myConf = sc.getConf.getAll
    for (cnf <- myConf)
      Console.println(cnf._1 +", "+ cnf._2)
    Console.println(sc.defaultParallelism)

    val path: String = inputs.head
    val pth = new Path(path)
    Console.println(pth)
    val inputRdd: RDD[(ProjectedExtent, Tile)] =
    sc.hadoopGeoTiffRDD(pth)

    val layoutScheme = FloatingLayoutScheme(512)

    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
      inputRdd.collectMetadata[SpatialKey](layoutScheme)

    val tilerOptions =
      Tiler.Options(
        resampleMethod = Average,
        partitioner = new RoundRobin(inputRdd.partitions.length)
        // partitioner = new SamePartitioner(inputRdd.partitions.length)
        // https://stackoverflow.com/questions/23127329/how-to-define-custom-partitioner-for-spark-rdds-of-equally-sized-partition-where
      )

    val tiledRdd =
      inputRdd.tileToLayout[SpatialKey](metadata, tilerOptions)


    // At this point, we want to combine our RDD and our Metadata to get a TileLayerRDD[SpatialKey]

    val layerRdd: TileLayerRDD[SpatialKey] =
      ContextRDD(tiledRdd, metadata)

    val areaOfInterest: Extent = Extent(300000.000, 6000000.000, 500000.000, 6100000.000)

    val cropedRDD = layerRdd.crop(areaOfInterest)
    val stitched = cropedRDD.stitch()

    val geotiff = GeoTiff(stitched, cropedRDD.metadata.crs)
    val pathHadoop = new Path(output + "_spark")
    geotiff.write(pathHadoop, cropedRDD.sparkContext.hadoopConfiguration)

    Console.println(inputRdd.partitions.length)

    // val geoTiff: SinglebandGeoTiff = GeoTiffReader.readSingleband(path)
    // val newGeoTiff: SinglebandGeoTiff = geoTiff.crop(4220, 4220, 4270, 4270)
    // GeoTiffWriter.write(newGeoTiff, output)

    // val resampleRasterExtent: RasterExtent = RasterExtent(
    //   geoTiff.extent,
    //   1000,
    //   1000
    // )
    // val resampled: SinglebandRaster = geoTiff.resample(resampleRasterExtent, Average, AutoHigherResolution)
    // val anotherGeoTiff: SinglebandGeoTiff = SinglebandGeoTiff(resampled.tile, resampled.extent, geoTiff.crs, geoTiff.tags, geoTiff.options, geoTiff.overviews)
    // GeoTiffWriter.write(anotherGeoTiff, output + "2")

    // val sumTiff: SinglebandGeoTiff = geoTiff + anotherGeoTiff
    // GeoTiffWriter.write(sumTiff, output + "3")
    // Console.println(geoTiff.extent)
    // val resampleRasterExtent: RasterExtent = RasterExtent(
    //   geoTiff.extent,
    //   1000,
    //   1000
    // )
    // val resampled: SinglebandRaster = geoTiff.resample(resampleRasterExtent, Average, AutoHigherResolution)
    // val anotherGeoTiff: SinglebandGeoTiff = SinglebandGeoTiff(resampled.tile, resampled.extent, geoTiff.crs, geoTiff.tags, geoTiff.options, geoTiff.overviews)
    // GeoTiffWriter.write(anotherGeoTiff, output + "2")

  }
}
