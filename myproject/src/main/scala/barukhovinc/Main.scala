package barukhovinc

import geotrellis.layer._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff._
import geotrellis.spark._

import cats.implicits._
import com.monovore.decline._

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._

import org.log4s._

object  Main {
  @transient private[this] lazy val logger = getLogger

  private val inputsOpt = Opts.options[String](
    "inputs", help = "The path that points to data that will be read")
  private val outputOpt = Opts.option[String](
    "output", help = "The path of the output tiffs")
  private val partitionsOpt =  Opts.option[Int](
    "numPartitions", help = "The number of partitions to use").orNone

  private val command = Command(name = "MyProject", header = "GeoTrellis App: MyProject") {
    (inputsOpt, outputOpt, partitionsOpt).tupled
  }

  def main(args: Array[String]): Unit = {
    command.parse(args, sys.env) match {
      case Left(help) =>
        System.err.println(help)
        sys.exit(1)

      case Right((i, o, p)) =>
        try {
          run(i.toList, o, p)(Spark.context)
        } finally {
          Spark.session.stop()
        }
    }
  }

  def run(inputs: List[String], output: String, numPartitions: Option[Int])(implicit sc: SparkContext): Unit = {
    val path: String = inputs.head
    val geoTiff: SinglebandGeoTiff = GeoTiffReader.readSingleband(path)
    val newGeoTiff: SinglebandGeoTiff = geoTiff.crop(4220, 4220, 4270, 4270)
    GeoTiffWriter.write(newGeoTiff, output)

    val resampleRasterExtent: RasterExtent = RasterExtent(
      geoTiff.extent,
      1000,
      1000
    )
    val resampled: SinglebandRaster = geoTiff.resample(resampleRasterExtent, Average, AutoHigherResolution)
    val anotherGeoTiff: SinglebandGeoTiff = SinglebandGeoTiff(resampled.tile, resampled.extent, geoTiff.crs, geoTiff.tags, geoTiff.options, geoTiff.overviews)
    GeoTiffWriter.write(anotherGeoTiff, output + "2")
  }
}
