package org.locationtech.geomesa.avro

import com.vividsolutions.jts.geom.Coordinate
import org.apache.avro.generic.{GenericArray, GenericRecord}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert.{TransformerFn, TransformerFunctionFactory}
import org.locationtech.geomesa.convert.Transformers.EvaluationContext

import scala.collection.JavaConversions._

object WayPointArray2LineString extends TransformerFn {

  private val gf = JTSFactoryFinder.getGeometryFactory

  override def names: Seq[String] = Seq("wayPoint2LS")

  override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
    val arr = args(1).asInstanceOf[GenericArray[GenericRecord]]
    val coords =
      arr.map { el =>
        val lat = el.get("lat").asInstanceOf[java.lang.Double]
        val lon = el.get("lon").asInstanceOf[java.lang.Double]
        new Coordinate(lon, lat)
      }
    gf.createLineString(coords.toArray)
  }
}

class WayPointFunctionFactory extends TransformerFunctionFactory {
  override def functions: Seq[TransformerFn] = Seq(WayPointArray2LineString)
}