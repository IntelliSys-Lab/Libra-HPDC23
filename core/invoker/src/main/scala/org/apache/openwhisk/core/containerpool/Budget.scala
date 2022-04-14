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

import org.apache.openwhisk.core.entity.MemoryLimit
import scala.collection.immutable.Map
import scala.collection.immutable.Vector
import java.time.Instant

//
// A budget for every activation
//

class Budget(self: Run) {
  var memoryIn = Map.empty[Run, Int]
  var memoryOut = Map.empty[Run, Int]
  var cpuIn = Map.empty[Run, Int]
  var cpuOut = Map.empty[Run, Int]
  var cpuDelta: Int = 0
  var memoryDelta: Int = 0
  var memoryIdleTime: Long = 0.toLong
  var cpuIdleTime: Long = 0.toLong
  var memoryTrajectory: String = ""
  var cpuTrajectory: String = ""

  var isSafeguard: Int = 0

  def setMemoryDelta(memory: Int): Unit = {
    memoryDelta = memory*MemoryLimit.MEM_UNIT
  }

  def setCpuDelta(cpu: Int): Unit = {
    cpuDelta = cpu
  }

  def addMemoryIn(list: Vector[(Run, Int, Long, Long)]): Unit = {
    for ((job, value, start, end) <- list) {
      memoryIn = memoryIn + memoryIn.get(job).map{m => 
        job -> (m + value)
      }.getOrElse(
        job -> value
      )

      if (memoryTrajectory == "") {
        memoryTrajectory = s"${job.msg.activationId.toString}:${value*MemoryLimit.MEM_UNIT}"
      } else {
        memoryTrajectory = memoryTrajectory + s" ${job.msg.activationId.toString}:${value*MemoryLimit.MEM_UNIT}"
      }
    }
  }

  def removeMemoryIn(list: Vector[Run]): Unit = {
    for (job <- list) {
      memoryIn = memoryIn - job
    }
  }

  def resetMemoryIn(): Unit = {
    memoryIn = Map.empty[Run, Int]
  }

  def addMemoryOut(list: Vector[(Run, Int, Long, Long)]): Unit = {
    for ((job, value, start, end) <- list) {
      memoryOut = memoryOut + memoryOut.get(job).map{m => 
        job -> (m + value)
      }.getOrElse(
        job -> value
      )
      
      memoryIdleTime = memoryIdleTime + (Instant.now.toEpochMilli - start) * (value*MemoryLimit.MEM_UNIT)

      if (memoryTrajectory == "") {
        memoryTrajectory = s"${job.msg.activationId.toString}:-${value*MemoryLimit.MEM_UNIT}"
      } else {
        memoryTrajectory = memoryTrajectory + s" ${job.msg.activationId.toString}:-${value*MemoryLimit.MEM_UNIT}"
      }
    }
  }

  def removeMemoryOut(list: Vector[Run]): Unit = {
    for (job <- list) {
      memoryOut = memoryOut - job
    }
  }

  def resetMemoryOut(left: Option[(Int, Long, Long)]): Unit = {
    memoryOut = Map.empty[Run, Int]

    // Update rest of idle time in harvested resource pool
    left match {
      case Some((value, start, end)) => memoryIdleTime = memoryIdleTime + (Instant.now.toEpochMilli - start) * (value*MemoryLimit.MEM_UNIT)
      case None =>
    }
  }

  def addCpuIn(list: Vector[(Run, Int, Long, Long)]): Unit = {
    for ((job, value, start, end) <- list) {
      cpuIn = cpuIn + cpuIn.get(job).map{m => 
        job -> (m + value)
      }.getOrElse(
        job -> value
      )

      if (cpuTrajectory == "") {
        cpuTrajectory = s"${job.msg.activationId.toString}:${value}"
      } else {
        cpuTrajectory = cpuTrajectory + s" ${job.msg.activationId.toString}:${value}"
      }
    }
  }

  def removeCpuIn(list: Vector[Run]): Unit = {
    for (job <- list) {
      cpuIn = cpuIn - job
    }
  }

  def resetCpuIn(): Unit = {
    cpuIn = Map.empty[Run, Int]
  }

  def addCpuOut(list: Vector[(Run, Int, Long, Long)]): Unit = {
    for ((job, value, start, end) <- list) {
      cpuOut = cpuOut + cpuOut.get(job).map{m => 
        job -> (m + value)
      }.getOrElse(
        job -> value
      )

      cpuIdleTime = cpuIdleTime + (Instant.now.toEpochMilli - start) * value

      if (cpuTrajectory == "") {
        cpuTrajectory = s"${job.msg.activationId.toString}:-${value}"
      } else {
        cpuTrajectory = cpuTrajectory + s" ${job.msg.activationId.toString}:-${value}"
      }
    }
  }

  def removeCpuOut(list: Vector[Run]): Unit = {
    for (job <- list) {
      cpuOut = cpuOut - job
    }
  }

  def resetCpuOut(left: Option[(Int, Long, Long)]): Unit = {
    cpuOut = Map.empty[Run, Int]

    // Update rest of idle time in harvested resource pool
    left match {
      case Some((value, start, end)) => cpuIdleTime = cpuIdleTime + (Instant.now.toEpochMilli - start) * value
      case None =>
    }
  }

  def totalIdleTime(): (String, String) = {((memoryIdleTime).toString, (cpuIdleTime).toString)}

  def summary(): (String, String) = {
    val id = self.msg.activationId.toString
    val (memoryIdle, cpuIdle) = totalIdleTime()
    val trajectory: String = s"${isSafeguard};${memoryDelta} ${cpuDelta};${memoryIdle} ${cpuIdle};${memoryTrajectory};${cpuTrajectory}"
    (id, trajectory)
  }
}
