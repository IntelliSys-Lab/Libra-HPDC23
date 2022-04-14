/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool

import scala.collection.immutable.Map
import scala.collection.immutable.Vector

class PriorityMap() {
  var map : Map[Run, (Int, Long, Long)] = Map[Run, (Int, Long, Long)]() // Invocation -> (harvested resource, start time, estimated end time)

  def insert(key: Run, value: Int, start: Long, end: Long): Unit = {
    map.get(key) match {
      case Some((v, start, end)) => map = map + (key -> (v + value, start, end))
      case None => map = map + (key -> (value, start, end))
    }
  }

  def remove(key: Run): Option[(Int, Long, Long)] = {
    val obj = map.get(key)
    map = map - key
    obj
  }

  def get(value: Int): (Int, Vector[(Run, Int, Long, Long)]) = {
    var res = Vector[(Run, Int, Long, Long)]()
    var consumed: Int = 0
    while (consumed < value && !map.isEmpty) {
      val current = map.maxBy(_._2._3)
      if (current._2._1 <= value - consumed) {
        res = res :+ (current._1, current._2._1, current._2._2, current._2._3)
        map = map - current._1
        consumed = consumed + current._2._1
      } else {
        res = res :+ (current._1, value - consumed, current._2._2, current._2._3)
        map.get(current._1) match {
          case Some((v, start, end)) => map = map + (current._1 -> (v - (value - consumed), start, end))
          case None =>
        }
        
        consumed = consumed + (value - consumed)
      }
    }
    (consumed, res)
  }

  def isEmpty: Boolean = {
    map.isEmpty
  }

  def summary: String = {
    var state: String = "None"
    map.map{
      case (k, (v, start, end)) => {
        if (start < end) {
          if (state == "None") {
            state = s"${k.msg.activationId.toString}:${v};${start};${end}"
          } else {
            state = state + s",${k.msg.activationId.toString}:${v};${start};${end}"
          }
        }
      }
    }
    state
  }
}
