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

import java.net.InetSocketAddress
import java.util.Collections

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TaxiRideQueries {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableClosureCleaner()
    val tEnv = TableEnvironment.getTableEnvironment(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    tEnv.registerFunction("inNyc", inNyc)
    tEnv.registerFunction("toCellId", toCellId)
    tEnv.registerFunction("toCoords", toCoords)
    tEnv.registerFunction("toGeoPoint", toGeoPoint)
    tEnv.registerFunction("toLong", toLong)

    // get the taxi ride data stream
    tEnv.registerTableSource(
      "rides",
      new TaxiRideTableSource("/Users/fhueske/nycTaxiRides.gz", 60, 600))

    // SCHEMA
//      rideId: Long,
//      isStart: Boolean,
//      startTime: Long,
//      endTime: Long,
//      startLon: Float,
//      startLat: Float,
//      endLon: Float,
//      endLat: Float,
//      passengerCnt: Short

    // QUERIES
//    departuresPerTenMinsTumble(tEnv)
//    departuresPerTenMinsTumbleSQL(tEnv)
//    freqDeptLocations(tEnv)

    departureCntFreq(tEnv)

    // run query
    env.execute()
  }

  def departuresPerTenMinsTumble(tEnv: StreamTableEnvironment): Unit = {

    val departuresCnts = tEnv.scan("rides")
      // keep start events from NYC
      .filter("isStart === true && inNyc(startLon, startLat)")
      // map departure location to cellId
      .select('rideId, toCellId('startLon, 'startLat) as 'cell, 'rowtime)
      // 10 minute tumble window
      .window(Tumble over 10.minutes on 'rowtime as 'w)
      // group by departure cell and window
      .groupBy('cell, 'w)
      // count departures
      .select(
        toGeoPoint(toCoords('cell)) as 'location, // geo point
        'w.end.cast(Types.LONG) as 'deptTime,     // timestamp
        'rideId.count as 'deptCnt)                // departure count

//    departuresCnts.toRetractStream[Row].print()

    val sink = new ElasticUpsertTableSink(
      Collections.emptyMap(),
      Collections.singletonList(new InetSocketAddress("localhost", 9300)),
      "taxi-rides",
      "departureCnts")

    departuresCnts.writeToSink(
      sink,
      tEnv.queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2)))

  }


  def departuresPerTenMinsTumbleSQL(tEnv: StreamTableEnvironment): Unit = {

    val q =
      """
        |SELECT location, toLong(deptTime) AS deptTime, deptCnt FROM (
        |  SELECT
        |    toGeoPoint(toCoords(toCellId(startLon, startLat))) AS location,
        |    toLong(TUMBLE_END(rowtime, INTERVAL '10' MINUTE)) AS deptTime,
        |    COUNT(rideId) as deptCnt
        |  FROM rides
        |  WHERE isStart AND inNyc(startLon, startLat)
        |  GROUP BY TUMBLE(rowtime, INTERVAL '10' MINUTE), toCellId(startLon, startLat)
        | )
      """.stripMargin

    val departuresCnts = tEnv.sql(q)

    val sink = new ElasticUpsertTableSink(
      Collections.emptyMap(),
      Collections.singletonList(new InetSocketAddress("localhost", 9300)),
      "taxi-rides",
      "departureCnts")

    departuresCnts.writeToSink(sink)
  }

  def freqDeptLocations(tEnv: StreamTableEnvironment): Unit = {

    val departuresCnts = tEnv.scan("rides")
      .filter('isStart === true && inNyc('startLon, 'startLat))
      // map departure location to cellId
      .select('rideId, toCellId('startLon, 'startLat) as 'cell, 'rowtime)
      // sliding OVER window of 10 mins preceding
      .window(Over partitionBy 'cell orderBy 'rowtime preceding 10.minutes as 'w)
      .select(
        toGeoPoint(toCoords('cell)) as 'location, // geo point
        'rowtime.cast(Types.LONG) as 'deptTime,   // timestamp
        'rideId.count over 'w as 'deptCnt)        // departure count of last 10 minutes
      // filter for frequent departure locations
      .filter('deptCnt > 10)

    departuresCnts.toDataStream[Row].print()
  }

  def departureCntFreq(tEnv: StreamTableEnvironment): Unit = {

    val departureFreq = tEnv.scan("rides")
      .filter('isStart && inNyc('startLon, 'startLat))
      .select('rideId, toCellId('startLon, 'startLat) as 'cell)
      .groupBy('cell)
      .select('cell, 'rideId.count as 'deptCnt)
      .groupBy('deptCnt)
      .select('deptCnt, 'cell.count as 'cntFreq)

//    departureFreq.toRetractStream[Row].print()

    val sink = new ElasticUpsertTableSink(
      Collections.emptyMap(),
      Collections.singletonList(new InetSocketAddress("localhost", 9300)),
      "taxi-rides2",
      "cntFreq")
    departureFreq.writeToSink(sink)

  }

}

