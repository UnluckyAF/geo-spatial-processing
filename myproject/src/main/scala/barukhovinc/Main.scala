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
    // val input1: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_SR_B5.TIF"
    // val input2: String = "file:/Users/barukhov/geo_spatial_data/LC09_L2SP_178022_20220412_20220414_02_T1/LC09_L2SP_178022_20220412_20220414_02_T1_QA_PIXEL.TIF"
    // val output: String = "/Users/barukhov/geo_spatial_data/res"
    // val opName: String = "add"
    val otherArgs: Array[String] = Array[String]("2")
    val input1: String = "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176021_20211231_20220107_01_T2/LC08_L1GT_176021_20211231_20220107_01_T2_B2.TIF"
    val input2: String = "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176021_20211231_20220107_01_T2/LC08_L1GT_176021_20211231_20220107_01_T2_B3.TIF"
    val input3: String = "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176021_20211231_20220107_01_T2/LC08_L1GT_176021_20211231_20220107_01_T2_B4.TIF"
    // val input1: String = "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_177022_20211120_20211130_01_T2/"
    // val prefix: String = "LC08_L1GT_177022_20211120_20211130_01_T2"
    // val input2: String = "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_177022_20211120_20211130_01_T2/LC08_L1GT_177022_20211120_20211130_01_T2_B2.TIF"
    // val input3: String = "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_177022_20211120_20211130_01_T2/LC08_L1GT_177022_20211120_20211130_01_T2_B4.TIF"
    val output: String = "/Users/barukhov/geo_spatial_data/res"
    val opName: String = "spectest4"
    run(List[String](input1, input2, input3), output, opName, otherArgs)(Spark.context)
    // Spark.session.stop()
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

    val layoutScheme = FloatingLayoutScheme(256)

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

    Console.println(tiledRdd.partitions.length)
    return (tiledRdd, metadata)
  }

  def simpleMultiRead(inputs: List[String])(implicit sc: SparkContext): (RDD[(SpatialKey,MultibandTile)], TileLayerMetadata[SpatialKey]) = {
    // var tileInputs: List[MultibandTileLayerRDD[SpatialKey]] = List[MultibandTileLayerRDD[SpatialKey]]()
    // var (resRDD: RDD[(SpatialKey,MultibandTile)], spec_metadata: TileLayerMetadata[SpatialKey]) = oneRead(inputs.head)(sc)

    // val layoutScheme = FloatingLayoutScheme(512)

    // for (path <- inputs.tail) {
    //   val (rdd: RDD[(SpatialKey,MultibandTile)], _: TileLayerMetadata[SpatialKey]) = oneRead(path)(sc)
    //   resRDD = resRDD.union(rdd)
    // }

    // val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
    //   resRDD.collectMetadata[SpatialKey](layoutScheme)

    // val layerRdd: MultibandTileLayerRDD[SpatialKey] =
    //   ContextRDD(resRDD, spec_metadata)
    // var res: MultibandTileLayerRDD[SpatialKey] = tileInputs.head
    // for (rdd <- tileInputs.tail) {
    //   res = res.union(rdd)
    // }
    val pth_start = new Path(inputs.head)
    Console.println(pth_start)
    var inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
    sc.hadoopMultibandGeoTiffRDD(pth_start)
    for (path <- inputs.tail) {
      val pth = new Path(path)
      Console.println(pth)
      val newInputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(pth)
      inputRdd = inputRdd.union(newInputRdd)
    }

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

    // val layerRdd: TileLayerRDD[SpatialKey] =
    //   ContextRDD(tiledRdd, metadata)

    // tileInputs = layerRdd :: tileInputs
    Console.println(inputRdd.partitions.length)
    // return layerRdd
    return (tiledRdd, metadata)

  }

  def multiRead(inputs: List[String])(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    // var tileInputs: List[MultibandTileLayerRDD[SpatialKey]] = List[MultibandTileLayerRDD[SpatialKey]]()
    var (resRDD: RDD[(SpatialKey,MultibandTile)], spec_metadata: TileLayerMetadata[SpatialKey]) = oneRead(inputs.head)(sc)

    val layoutScheme = FloatingLayoutScheme(256)

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

  class Scene_L8C1L1(input: String, prefix: String) {
    val b: String = "B2.TIF"
    val g: String = "B3.TIF"
    val r: String = "B4.TIF"
    val nir: String = "B5.TIF"
    val qa: String = "BQA.TIF"
    val dir: String = input

    var rgb: MultibandTileLayerRDD[SpatialKey] = _
    var is_rgb: Boolean = false

    def getRGB()(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
      if (is_rgb) {
        return rgb
      }
      val res = multiRead(List[String](dir+prefix+"_"+r, dir+prefix+"_"+g,dir+prefix+"_"+b))(sc)
      is_rgb = true
      rgb = res
      return res
    }

    def getRGNIR()(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
      val res = multiRead(List[String](dir+prefix+"_"+r, dir+prefix+"_"+g,dir+prefix+"_"+nir))(sc)
      return res
    }

    def getQA()(implicit sc: SparkContext): TileLayerRDD[SpatialKey] = {
      val res = read(List[String](dir+prefix+"_"+qa))(sc)
      return res.head
    }
  }

  // class Scenes_L8C1L1(input: String, prefix: String) {
  //   val b: String = "B2.TIF"
  //   val g: String = "B3.TIF"
  //   val r: String = "B4.TIF"
  //   val nir: String = "B5.TIF"
  //   val qa: String = "BQA.TIF"
  //   val dir: String = input

  //   var rgb: MultibandTileLayerRDD[SpatialKey] = _
  //   var is_rgb: Boolean = false

  //   def getRGB()(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
  //     if (is_rgb) {
  //       return rgb
  //     }
  //     val res = multiRead(List[String](dir+prefix+"_"+r, dir+prefix+"_"+g,dir+prefix+"_"+b))(sc)
  //     is_rgb = true
  //     rgb = res
  //     return res
  //   }

  //   def getRGNIR()(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
  //     val res = multiRead(List[String](dir+prefix+"_"+r, dir+prefix+"_"+g,dir+prefix+"_"+nir))(sc)
  //     return res
  //   }

  //   def getQA()(implicit sc: SparkContext): TileLayerRDD[SpatialKey] = {
  //     val res = read(List[String](dir+prefix+"_"+qa))(sc)
  //     return res.head
  //   }
  // }

  def removeClouds(layerRdd: MultibandTileLayerRDD[SpatialKey])(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val input4: String = "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_177022_20211120_20211130_01_T2/LC08_L1GT_177022_20211120_20211130_01_T2_BQA.TIF"
    val pth = new Path(input4)
    val qaResRDD: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(pth)
    // var (qaResRDD: RDD[(SpatialKey,MultibandTile)], _: TileLayerMetadata[SpatialKey]) = oneRead(input4)(sc)
    // val qaLayerRdd: MultibandTileLayerRDD[SpatialKey] =
    //   ContextRDD(qaResRDD, qa_spec_metadata)
    def maskClouds(tile: Tile, qaTile: Tile): Tile =
      tile.combine(qaTile) { (v: Int, qa: Int) =>
        val isCloud = qa & 0x0008
        val isCirrus = qa & 0x1800
        Console.println(isCloud, isCirrus)
        if(isCloud > 0 || isCirrus > 0) { NODATA }
        else { v }
      }

    val masked = layerRdd.withContext { rdd =>
      // keys should match though
      rdd.mapValues {case mbt =>
      //   case ((_: SpatialKey, mbt: MultibandTile), (_: ProjectedExtent, qa: Tile)) =>
        mbt.mapBands { case (_, b) => maskClouds(b, qaResRDD.first._2) }
      }
    }
      // tile.combine(qaLayerRdd) { (v: Int, qa: Int) =>
      //   val isCloud = qa & 0x8000
      //   val isCirrus = qa & 0x2000
      //   if(isCloud > 0 || isCirrus > 0) { NODATA }
      //   else { v }
      // }

    // val masked = maskClouds(layerRdd)
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

  def multiCrop(tile: MultibandTileLayerRDD[SpatialKey], boundingBox: ArraySeq[Double]): MultibandTileLayerRDD[SpatialKey] = {
    val minX: Double = boundingBox(0)
    val minY: Double = boundingBox(1)
    val maxX: Double = boundingBox(2)
    val maxY: Double = boundingBox(3)
    val areaOfInterest: Extent = Extent(minX, minY, maxX, maxY)

    val cropedRDD = tile.crop(areaOfInterest)
    return cropedRDD
  }

  def multiAdd(tileInputs: List[MultibandTileLayerRDD[SpatialKey]]): MultibandTileLayerRDD[SpatialKey] = {
    val first = tileInputs.head
    val second = tileInputs.last
    val res = first.withContext {
      rdd => rdd.zip(second).map {
        case ((key, fst_mbnd), (_, scnd_mbnd)) => (key, fst_mbnd.mapBands {
          (ind, tile) => tile + scnd_mbnd.band(ind)
        })
      }
    }

    return res
  }

  def multiFocalSum(tileInputs: List[MultibandTileLayerRDD[SpatialKey]], padding: Int): MultibandTileLayerRDD[SpatialKey] = {
    val first = tileInputs.head
    val neighborhood = Square(padding)
    val res = first.withContext {
      rdd => rdd.bufferTiles(neighborhood.extent).mapValues {
        bufferedTile => bufferedTile.tile.mapBands {
          (_, band) => band.focalSum(neighborhood, Some(bufferedTile.targetArea))
        }
      }
    }

    return res
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
        multiWrite(tileInputs, output)
      case "spectest" =>
        val scene = new Scene_L8C1L1(inputs.head, inputs.tail.head)
        // val (rdd: RDD[(SpatialKey,MultibandTile)], meta: TileLayerMetadata[SpatialKey]) = oneRead(inputs.head)(sc)
        // val layerRdd: MultibandTileLayerRDD[SpatialKey] =
        //   ContextRDD(rdd, meta)
        // // val tileInputs: MultibandTileLayerRDD[SpatialKey] = multiRead(inputs)(sc)
        // // val res = removeClouds(tileInputs)
        // val res = layerRdd.withContext{
        //   rdd => rdd.mapValues{
        //     tile => tile.subsetBands(3, 4, 5)
        //   }
        // }
        val res = scene.getRGB()(sc)
        multiWrite(res, output)
      case "spectest2" =>
        // val (rdd, meta) = simpleMultiRead(inputs)(sc)
        val inputs2 = List[String]("file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B2.TIF",
          "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B3.TIF",
          "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B4.TIF"
        )
        val inputs3 = List[String]("file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B2.TIF",
          "file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B3.TIF",
          "file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B4.TIF"
        )


        // Constructs a ColorMap with default options,
        // and a set of mapped values to color stops.
        // val colorMap1 =
        //   ColorMap(
        //     Map(
        //       3.5 -> RGB(0,255,0),
        //       7.5 -> RGB(63,255,51),
        //       11.5 -> RGB(102,255,102),
        //       15.5 -> RGB(178,255,102),
        //       19.5 -> RGB(255,255,0),
        //       23.5 -> RGB(255,255,51),
        //       26.5 -> RGB(255,153,51),
        //       31.5 -> RGB(255,128,0),
        //       35.0 -> RGB(255,51,51),
        //       40.0 -> RGB(255,0,0)
        //     )
        //   )

        val (rdd2, meta2) = simpleMultiRead(inputs ::: inputs2 ::: inputs3)(sc)
        // val res = rdd.union(rdd2)

        val layerRdd: MultibandTileLayerRDD[SpatialKey] =
          ContextRDD(rdd2, meta2)

        // val res = layerRdd.map {
        //   case (key, tile) => {
        //     (key, tile.renderPng().write("/Users/barukhov/geo_spatial_data/check_rgb"))
        //   }
        // }
        val res = multiCrop(layerRdd, ArraySeq[Double](450000.000, 6000000.000, 750000.000, 6200000.000))
        multiWrite(res, output)
      case "spectest3" =>
        // val (rdd, meta) = simpleMultiRead(inputs)(sc)
        val inputs2 = List[String]("file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B2.TIF",
          "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B3.TIF",
          "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B4.TIF"
        )
        // val inputs3 = List[String]("file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B2.TIF",
        //   "file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B3.TIF",
        //   "file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B4.TIF"
        // )
        val f = multiRead(inputs)(sc)
        val s = multiRead(inputs2)(sc)
        val res = multiAdd(List[MultibandTileLayerRDD[SpatialKey]](f, s))
        multiWrite(res, output)
      case "spectest4" =>
        // val (rdd, meta) = simpleMultiRead(inputs)(sc)
        // val inputs2 = List[String]("file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B2.TIF",
        //   "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B3.TIF",
        //   "file:/Users/barukhov/geo_spatial_data/LC08_L1GT_176022_20211231_20220107_01_T2/LC08_L1GT_176022_20211231_20220107_01_T2_B4.TIF"
        // )
        // val inputs3 = List[String]("file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B2.TIF",
        //   "file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B3.TIF",
        //   "file:/Users/barukhov/geo_spatial_data/LC08_L1TP_178021_20211229_20220106_01_T2/LC08_L1TP_178021_20211229_20220106_01_T2_B4.TIF"
        // )
        val f = multiRead(inputs)(sc)
        // val s = multiRead(inputs2)(sc)
        val res = multiFocalSum(List[MultibandTileLayerRDD[SpatialKey]](f), otherArgs(0).toInt)
        multiWrite(res, output)
    }
// 300000.000, 6000000.000, 500000.000, 6100000.000

    // val addRes = layerRdd +

  }
}
