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

import com.dataartisans.table.demo.rides.{TaxiRide, TaxiRideSource}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.sources.{DefinedRowtimeAttribute, StreamTableSource}

class TaxiRideTableSource(
    val path: String,
    val maxOutOfOrder: Int,
    val servingSpeed: Int)
  extends StreamTableSource[TaxiRide]
    with DefinedRowtimeAttribute {

  override def getDataStream(env: StreamExecutionEnvironment): DataStream[TaxiRide] = {

    env.addSource(
      new TaxiRideSource(path, maxOutOfOrder, servingSpeed),
      TypeExtractor.getForClass(classOf[TaxiRide]))

  }

  override def getRowtimeAttribute: String = "rowtime"

  override def getReturnType: TypeInformation[TaxiRide] = {
    TypeExtractor.getForClass(classOf[TaxiRide])
  }

}
