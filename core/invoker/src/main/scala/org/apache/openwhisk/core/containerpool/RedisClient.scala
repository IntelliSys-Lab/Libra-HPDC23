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
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Response, JedisPool}


class RedisClient(
  host: String = "172.17.0.1",
  port: Int = 6379,
  password: String = "openwhisk",
  database: Int = 0
) {
  private var pool: JedisPool = _
  val interval: Int = 100 // ms

  def init: Unit = {
    val maxTotal: Int = 300
    val maxIdle: Int = 100
    val minIdle: Int = 1
    val timeout: Int = 30000

    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setMinIdle(minIdle)

    pool = new JedisPool(poolConfig, host, port, timeout, password, database)
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }

  //
  // Send observations to Redis
  //

  def setActivations(activations: Int): Boolean = {
    try {
      val jedis = pool.getResource
      val key: String = "n_undone_request"
      val value: String = activations.toString
      jedis.set(key, value)
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def setAvailableCpu(permits: Int): Boolean = {
    try {
      val jedis = pool.getResource
      val key: String = "available_cpu"
      val value: String = permits.toString
      jedis.set(key, value)
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def setAvailableMemory(permits: Int): Boolean = {
    try {
      val jedis = pool.getResource
      val key: String = "available_memory"
      val value: String = permits.toString
      jedis.set(key, value)
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def setActivationsForInvoker(invoker: String, activations: Int): Boolean = {
    try {
      val jedis = pool.getResource
      val name: String = invoker
      val key: String = "n_undone_request"
      val value: String = activations.toString
      jedis.hset(name, key, value)
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def setAvailableResourceForInvoker(
    invokerList: IndexedSeq[String], 
    memoryPermitsList: IndexedSeq[Int],
    cpuPermitsList: IndexedSeq[Int]
  ): Boolean = {
    try {
      val jedis = pool.getResource
      val pipeline = jedis.pipelined()
      val numInovkers: Int = invokerList.size
      for (i <- 0 until numInovkers) {
        val name: String = invokerList(i)
        pipeline.hset(name, "available_memory", memoryPermitsList(i).toString)
        pipeline.hset(name, "available_cpu", cpuPermitsList(i).toString)
      }
      pipeline.sync()
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def setSummaryForInvoker(invokerName: String, memorySummary: String, cpuSummary: String): Boolean = {
    try {
      val jedis = pool.getResource
      val pipeline = jedis.pipelined()
      pipeline.hset(invokerName, "memory_summary", memorySummary)
      pipeline.hset(invokerName, "cpu_summary", cpuSummary)
      pipeline.sync()
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def getSummaryForInvoker(numInvokers: Int): (IndexedSeq[Map[String, (Int, Long, Long)]], IndexedSeq[Map[String, (Int, Long, Long)]]) = {
    try {
      val jedis = pool.getResource
      val pipeline = jedis.pipelined()
      var result: IndexedSeq[(Response[String], Response[String])] = IndexedSeq[(Response[String], Response[String])]()
      var memorySummaryList: IndexedSeq[Map[String, (Int, Long, Long)]] = IndexedSeq[Map[String, (Int, Long, Long)]]()
      var cpuSummaryList: IndexedSeq[Map[String, (Int, Long, Long)]] = IndexedSeq[Map[String, (Int, Long, Long)]]()

      for (i <- 0 until numInvokers) {
        val name: String = "invoker" + i.toString
        result = result :+ (pipeline.hget(name, "memory_summary"), pipeline.hget(name, "cpu_summary"))
      }
      pipeline.sync()

      result.map{
        case(m, c) => {
          val memorySummary = m.get()
          val cpuSummary = c.get()
          var memoryMap = Map.empty[String, (Int, Long, Long)]
          var cpuMap = Map.empty[String, (Int, Long, Long)]

          if (memorySummary != "None") {
            val objList = memorySummary.split(",")
            for (i <- 0 until objList.length) {
              val key = objList(i).split(":")(0)
              val valueList = objList(i).split(":")(1).split(";")
              val value = valueList(0).toInt
              val start = valueList(1).toLong
              val end = valueList(2).toLong
              memoryMap = memoryMap + (key -> (value, start, end))
            }
          }
          if (cpuSummary != "None") {
            val objList = cpuSummary.split(",")
            for (i <- 0 until objList.length) {
              val key = objList(i).split(":")(0)
              val valueList = objList(i).split(":")(1).split(";")
              val value = valueList(0).toInt
              val start = valueList(1).toLong
              val end = valueList(2).toLong
              cpuMap = cpuMap + (key -> (value, start, end))
            }
          }

          memorySummaryList = memorySummaryList :+ memoryMap 
          cpuSummaryList = cpuSummaryList :+ cpuMap 
        }
      }
      jedis.close()
      (memorySummaryList, cpuSummaryList)
    } catch {
      case e: Exception => {
        (IndexedSeq[Map[String, (Int, Long, Long)]](), IndexedSeq[Map[String, (Int, Long, Long)]]())
      }
    }
  }

  def getMWSLoadForInvoker(numInvokers: Int): IndexedSeq[Double] = {
    try {
      val jedis = pool.getResource
      val pipeline = jedis.pipelined()
      var result: IndexedSeq[Response[String]] = IndexedSeq[Response[String]]()
      var mwsLoadList: IndexedSeq[Double] = IndexedSeq[Double]()

      for (i <- 0 until numInvokers) {
        val name: String = "invoker" + i.toString
        result = result :+ pipeline.hget(name, "mws_load")
      }
      pipeline.sync()

      result.map{
        case s => {mwsLoadList = mwsLoadList :+ s.get().toDouble}
      }
      jedis.close()
      mwsLoadList
    } catch {
      case e: Exception => {
        IndexedSeq[Double]()
      }
    }
  }

  def setTrajectoryForInvocation(id: String, trajectory: String): Boolean = {
    try {
      val jedis = pool.getResource
      val name: String = "invocations"
      val key: String = id
      val value: String = trajectory
      jedis.hset(name, key, value)
      jedis.close()
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def getPredictPeakForFunction(id: String): Int = {
    try {
      val jedis = pool.getResource
      val name: String = "predict_peak"
      val key: String = id
      val predict_peak = jedis.hget(name, key).toInt
      jedis.close()
      predict_peak
    } catch {
      case e: Exception => {
        0
      }
    }
  }

  def getPredictDurationForFunction(id: String): Long = {
    try {
      val jedis = pool.getResource
      val name: String = "predict_duration"
      val key: String = id
      val predict_duration = jedis.hget(name, key).toLong
      jedis.close()
      predict_duration
    } catch {
      case e: Exception => {
        0.toLong
      }
    }
  }
}
