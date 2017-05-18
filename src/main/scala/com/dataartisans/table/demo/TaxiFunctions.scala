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

import java.sql.Timestamp

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.table.functions.ScalarFunction

object NycConstants {

  val LON_EAST = -73.7
  val LON_WEST = -74.05
  val LAT_NORTH = 41.0
  val LAT_SOUTH = 40.5

  val DELTA_LON = 0.0014
  val DELTA_LAT = 0.00125

  val NUMBER_OF_GRID_X = 250

}

object inNyc extends ScalarFunction {

  def eval(lon: Float, lat: Float): Boolean = {
    !(lon > NycConstants.LON_EAST || lon < NycConstants.LON_WEST) &&
      !(lat > NycConstants.LAT_NORTH || lat < NycConstants.LAT_SOUTH)
  }
}

object toCellId extends ScalarFunction {

  def eval(lon: Float, lat: Float): Int = {
    val xIndex: Int =
      Math.floor((Math.abs(NycConstants.LON_WEST) - Math.abs(lon)) / NycConstants.DELTA_LON).toInt
    val yIndex: Int =
      Math.floor((NycConstants.LAT_NORTH - lat) / NycConstants.DELTA_LAT).toInt

    xIndex + (yIndex * NycConstants.NUMBER_OF_GRID_X)
  }
}

object toCoords extends ScalarFunction {

  def eval(cellId: Int): (Float, Float) = {
    val xIdx = cellId % NycConstants.NUMBER_OF_GRID_X

    val xIndex = cellId % NycConstants.NUMBER_OF_GRID_X
    val yIndex = (cellId - xIndex) / NycConstants.NUMBER_OF_GRID_X

    val lon =
      (Math.abs(NycConstants.LON_WEST) -
        (xIndex * NycConstants.DELTA_LON) -
        (NycConstants.DELTA_LON / 2)
      ) * -1.0f
    val lat =
      NycConstants.LAT_NORTH - (yIndex * NycConstants.DELTA_LAT) - (NycConstants.DELTA_LAT / 2)

    (lon.toFloat, lat.toFloat)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    createTypeInformation[(Float, Float)].asInstanceOf[TypeInformation[_]]
}

object hourOfDay extends ScalarFunction {

  def eval(ts: Long): Long = ts % (24 * 60 * 60 * 1000)
}

object toLong extends ScalarFunction {

  def eval(ts: Timestamp): Long = ts.getTime
}