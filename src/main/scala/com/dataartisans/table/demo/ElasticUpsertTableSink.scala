/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.table.demo

import java.lang.{Boolean => JBool}
import java.net.InetSocketAddress

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream => JDStream}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.sinks.{TableSink, UpsertStreamTableSink}
import org.apache.flink.types.Row
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest

import scala.collection.JavaConverters._

class ElasticUpsertTableSink(
    config: java.util.Map[String, String],
    transportAddresses: java.util.List[InetSocketAddress],
    index: String,
    tpe: String)
  extends UpsertStreamTableSink[Row] {

  var fieldTypes: Array[TypeInformation[_]] = _
  var fieldNames: Array[String] = _

  var keys: Array[String] = _
  var appendOnly: Boolean = _

  override def setKeyFields(keys: Array[String]): Unit =
    // if not manually set by user, set extracted keys
    if (this.keys == null) {
      this.keys = keys
    }

  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(fieldTypes, fieldNames)

  override def getFieldNames: Array[String] = fieldNames

  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[JTuple2[JBool, Row]] = {

    val copy = new ElasticUpsertTableSink(config, transportAddresses, index, tpe)
    copy.fieldNames = fieldNames
    copy.fieldTypes = fieldTypes

    copy.asInstanceOf[TableSink[JTuple2[JBool, Row]]]
  }

  override def emitDataStream(dataStream: JDStream[JTuple2[JBool, Row]]): Unit = {

    val f: ElasticsearchSinkFunction[JTuple2[JBool, Row]] = if (appendOnly) {
      new RowAppendElasticSinkFunction(index, tpe, fieldNames)
    } else {
      new RowUpsertElasticSinkFunction(index, tpe, fieldNames, keys.map(fieldNames.indexOf(_)))
    }

    val sink = new RowElasticSearchSink(config, transportAddresses, f)
    dataStream.addSink(sink)
  }

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = this.appendOnly = isAppendOnly

}

class RowElasticSearchSink(
    userConfig: java.util.Map[String, String],
    transportAddresses: java.util.List[InetSocketAddress],
    f: ElasticsearchSinkFunction[JTuple2[JBool, Row]])
  extends ElasticsearchSink[JTuple2[JBool, Row]](userConfig, transportAddresses, f)

class RowAppendElasticSinkFunction(
  index: String,
  tpe: String,
  fieldNames: Array[String])
  extends ElasticsearchSinkFunction[JTuple2[JBool, Row]] {

  val fields: Array[(String, Int)] = fieldNames.zipWithIndex

  override def process(
      t: JTuple2[JBool, Row],
      runtimeContext: RuntimeContext,
      requestIndexer: RequestIndexer): Unit = {

    val json: Map[String, Any] =
      fields.foldLeft(Map[String, Any]())((m, x) => m + (x._1 -> t.f1.getField(x._2)))

    requestIndexer.add(new IndexRequest(index, tpe).source(json.asJava))
  }
}

class RowUpsertElasticSinkFunction(
    index: String,
    tpe: String,
    fieldNames: Array[String],
    keyFields: Array[Int])
  extends ElasticsearchSinkFunction[JTuple2[JBool, Row]] {

  val fields: Array[(String, Int)] = fieldNames.zipWithIndex

  def getKey(r: Row): String = keyFields.map(r.getField).mkString(":")

  def createIndexRequest(r: Row): UpdateRequest = {
    val key = getKey(r)
    val json: Map[String, Any] =
      fields.foldLeft(Map[String, Any]())((m, x) => m + (x._1 -> r.getField(x._2)))

    val idxReq = new IndexRequest(index, tpe, key)
      .source(json.asJava)

    new UpdateRequest(index, tpe, key)
      .doc(json.asJava)
      .upsert(idxReq)
  }

  def createDeleteRequest(r: Row): DeleteRequest = {
    new DeleteRequest(index, tpe, getKey(r))
  }

  override def process(t: JTuple2[JBool, Row], runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    val req = if (t.f0) createIndexRequest(t.f1) else createDeleteRequest(t.f1)
    requestIndexer.add(req)
  }
}

object toGeoPoint extends ScalarFunction {
  def eval(lon: Float, lat: Float): GeoPoint = GeoPoint(lon, lat)
  def eval(lon: Double, lat: Double): GeoPoint = GeoPoint(lon.toFloat, lat.toFloat)
  def eval(coord: (Float, Float)): GeoPoint = GeoPoint(coord._1, coord._2)
}

case class GeoPoint(lon: Float, lat: Float) {
  override def toString: String = s"$lat,$lon"
}
