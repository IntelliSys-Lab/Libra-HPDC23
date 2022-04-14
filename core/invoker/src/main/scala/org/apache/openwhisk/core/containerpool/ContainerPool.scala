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

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import org.apache.openwhisk.common.MetricEmitter
import org.apache.openwhisk.common.{AkkaLogging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.Try
import java.time.Instant
import scala.io.Source

sealed trait WorkerState
case object Busy extends WorkerState
case object Free extends WorkerState

case class WorkerData(data: ContainerData, state: WorkerState)

case object EmitMetrics

/**
 * A pool managing containers to run actions on.
 *
 * This pool fulfills the other half of the ContainerProxy contract. Only
 * one job (either Start or Run) is sent to a child-actor at any given
 * time. The pool then waits for a response of that container, indicating
 * the container is done with the job. Only then will the pool send another
 * request to that container.
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 *
 * @param childFactory method to create new container proxy actor
 * @param feed actor to request more work from
 * @param prewarmConfig optional settings for container prewarming
 * @param poolConfig config for the ContainerPool
 */
class ContainerPool(invokerName: String,
                    childFactory: ActorRefFactory => ActorRef,
                    feed: ActorRef,
                    prewarmConfig: List[PrewarmingConfig] = List.empty,
                    poolConfig: ContainerPoolConfig)
    extends Actor {
  import ContainerPool.memoryConsumptionOf
  import ContainerPool.cpuConsumptionOf

  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ec = context.dispatcher

  var freePool = immutable.Map.empty[ActorRef, ContainerData]
  var busyPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmedPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmStartingPool = immutable.Map.empty[ActorRef, (String, ByteSize)]

  // Harvested resource pools (HRP) for CPU and memory: job -> (amount of harvested resource, start time, predicted end time)
  var cpuHRP: PriorityMap = new PriorityMap()
  var memoryHRP: PriorityMap = new PriorityMap()

  // Record each activation to the container that forwarded to
  var record = immutable.Map.empty[Run, Container]

  // Watchdog for all activations
  var watchdog = immutable.Map.empty[Run, Budget]

  //
  // Redis interface
  //
  val redisHost: String = Source.fromFile("/config.libra").getLines.toList(0).split("=")(1)
  val redisPort: Int = 6379
  val redisPassword: String = "openwhisk"
  val redisDatabase: Int = 0

  private val redis = new RedisClient(redisHost, redisPort, redisPassword, redisDatabase)
  redis.init

  class RedisThread(
    redis: RedisClient
  ) extends Thread {
    override def run(){ 
      while (true) {
        Thread.sleep(redis.interval)

        // Update harvested resource pool
        redis.setSummaryForInvoker(invokerName, memoryHRP.summary, cpuHRP.summary)
      }
    }
  }

  // Start monitoring states
  val redisThread = new RedisThread(redis)
  redisThread.start()

  // If all memory slots are occupied and if there is currently no container to be removed, than the actions will be
  // buffered here to keep order of computation.
  // Otherwise actions with small memory-limits could block actions with large memory limits.
  var runBuffer = immutable.Queue.empty[Run]
  // Track the resent buffer head - so that we don't resend buffer head multiple times
  var resent: Option[Run] = None
  val logMessageInterval = 10.seconds
  //periodically emit metrics (don't need to do this for each message!)
  context.system.scheduler.schedule(30.seconds, 10.seconds, self, EmitMetrics)

  backfillPrewarms(true)

  def logContainerStart(r: Run, containerState: String, activeActivations: Int, container: Option[Container]): Unit = {
    val namespaceName = r.msg.user.namespace.name
    val actionName = r.action.name.name
    val maxConcurrent = r.action.limits.concurrency.maxConcurrent
    val activationId = r.msg.activationId.toString

    r.msg.transid.mark(
      this,
      LoggingMarkers.INVOKER_CONTAINER_START(
        containerState,
        r.msg.user.namespace.toString,
        r.msg.action.namespace.toString,
        r.msg.action.name.toString),
      s"containerStart containerState: $containerState container: $container activations: $activeActivations of max $maxConcurrent action: $actionName namespace: $namespaceName activationId: $activationId",
      akka.event.Logging.InfoLevel)
  }

  def receive: Receive = {
    // A job to run on a container
    //
    // Run messages are received either via the feed or from child containers which cannot process
    // their requests and send them back to the pool for rescheduling (this may happen if "docker" operations
    // fail for example, or a container has aged and was destroying itself when a new request was assigned)
    case r: Run =>
      // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
      val isResentFromBuffer = runBuffer.nonEmpty && runBuffer.dequeueOption.exists(_._1.msg == r.msg)
      
      /*** Libra's harvesting&reassignment algorithm ***/
      // Re-assign resources for a new job
      if (!isResentFromBuffer && r.predictPeak == 0) {
        val actionName = r.action.name.name

        // Fetch predict peak from Redis
        val predictPeak = redis.getPredictPeakForFunction(actionName)
        if (predictPeak == 0) {
          logging.error(this, s"getting action ${actionName} failed with predict peak 0, default unchanged")
          r.predictPeak = r.action.limits.memory.megabytes
        } else {
          logging.info(this, s"fetching action ${actionName} predict peak ${predictPeak}")
          r.predictPeak = predictPeak
        }

        // Fetch predict duration from Redis
        val predictDuration: Long = redis.getPredictDurationForFunction(actionName) // ms
        if (predictDuration == 0.toLong) {
          logging.error(this, s"getting action ${actionName} failed with predict duration 0, default set to 0")
          r.predictDuration = 0.toLong
        } else {
          logging.info(this, s"fetching action ${actionName} predict duration ${predictDuration}")
          r.predictDuration = predictDuration
        }

        // Determine harvest or accelerate
        val (memory, cpu) = (MemoryLimit.decodeMemory(r.predictPeak), MemoryLimit.decodeCpu(r.predictPeak))
        val (userMemory, userCpu) = (MemoryLimit.decodeMemory(r.action.limits.memory.megabytes), MemoryLimit.decodeCpu(r.action.limits.memory.megabytes))
        logging.info(this, s"job ${r}: predict memory ${memory}, predict cpu ${cpu}, user memory ${userMemory}, user cpu ${userCpu}")

        val budget = new Budget(r)
        watchdog = watchdog + (r -> budget)
        logging.info(this, s"init job ${r} with a budget ${budget}")

        // Set actual start time of this job
        r.startTime = Instant.now.toEpochMilli
        logging.info(this, s"Function ${actionName} starts accessing pool at ${Instant.now.toEpochMilli}")

        // For memory
        // If this activation can be harvested
        if (userMemory > memory) {
          memoryHRP.insert(r, (userMemory - memory) / MemoryLimit.MEM_UNIT, r.startTime, r.startTime + r.predictDuration)
          watchdog.get(r).foreach{b => b.setMemoryDelta(-(userMemory - memory) / MemoryLimit.MEM_UNIT)}
          r.actualMemory = memory
          logging.info(this, s"job ${r} user memory > predict memory, insert ${(userMemory - memory) / MemoryLimit.MEM_UNIT} to pool, set actual memory to ${r.actualMemory}")
        // If this activation can be accelerated
        } else if (userMemory < memory) { 
          // If memory HRP has idle resources, FCFS serving every activation
          if (!memoryHRP.isEmpty) {
            val (consumed, list) = memoryHRP.get((memory - userMemory) / MemoryLimit.MEM_UNIT) // Potential improvement: specify exact resource to dequeue instead of relying on priority
            // Update dst invocation
            watchdog.get(r).foreach{
              b => {
                b.addMemoryIn(list)
                b.setMemoryDelta(consumed)
              }
            }
            // Update src invocations
            list.map{
              case(k, v, start, end) => {
                watchdog.get(k).foreach{b => b.addMemoryOut(Vector((r, v, start, end)))}
                logging.info(this, s"record memory idle time for ${r}: dequeue time ${Instant.now.toEpochMilli}, enqueue time ${start}, with value ${v}")
              }
            }
            r.actualMemory = userMemory + consumed * MemoryLimit.MEM_UNIT
            logging.info(this, s"job ${r} user memory < predict memory, borrow ${consumed} from pool, dequeue list ${list}, set actual memory to ${r.actualMemory}")
          } else {
            r.actualMemory = userMemory
            logging.info(this, s"job ${r} user memory < predict memory, but the pool is empty, do nothing")
          }
        // If this activation is configured perfectly
        } else { 
          r.actualMemory = memory
          logging.info(this, s"job ${r} user memory = predict memory")
        }

        // For cpu
        // If this activation can be harvested
        if (userCpu > cpu) {
          cpuHRP.insert(r, userCpu - cpu, r.startTime, r.startTime + r.predictDuration)
          watchdog.get(r).foreach{b => b.setCpuDelta(-(userCpu - cpu))}
          r.actualCpu = cpu
          logging.info(this, s"job ${r} user cpu > predict cpu, insert ${userCpu - cpu} to pool, set actual memory to ${r.actualCpu}")
        // If this activation can be accelerated
        } else if (userCpu < cpu) {
          // If memory HRP has idle resources, FCFS serving every activation
          if (!cpuHRP.isEmpty) {
            val (consumed, list) = cpuHRP.get(cpu - userCpu) // Potential improvement: specify exact resource to dequeue instead of relying on priority
            // Update dst invocation
            watchdog.get(r).foreach{
              b => {
                b.addCpuIn(list)
                b.setCpuDelta(consumed)
              }
            }
            // Update src invocations
            list.map{
              case(k, v, start, end) => {
                watchdog.get(k).foreach{b => b.addCpuOut(Vector((r, v, start, end)))}
                logging.info(this, s"record cpu idle time for ${r}: dequeue time ${Instant.now.toEpochMilli}, enqueue time ${start}, with value ${v}")
              }
            }
            r.actualCpu = userCpu + consumed
            logging.info(this, s"job ${r} user cpu < predict cpu, borrow ${consumed} from pool, dequeue list ${list}, set actual cpu to ${r.actualCpu}")
          } else {
            r.actualCpu = userCpu
            logging.info(this, s"job ${r} user cpu < predict cpu, but the pool is empty, do nothing")
          }
        // If this activation is configured perfectly
        } else { 
          r.actualCpu = cpu
          logging.info(this, s"job ${r} user cpu = predict cpu")
        }

        logging.info(this, s"Function ${actionName} ends accessing pool at ${Instant.now.toEpochMilli}")
      }

      // Only process request, if there are no other requests waiting for free slots, or if the current request is the
      // next request to process
      // It is guaranteed, that only the first message on the buffer is resent.
      if (runBuffer.isEmpty || isResentFromBuffer) {
        if (isResentFromBuffer) {
          //remove from resent tracking - it may get resent again, or get processed
          resent = None
        }
        val createdContainer =
          // Is there enough space on the invoker for this action to be executed.
          if (hasPoolSpaceFor(busyPool, r.actualMemory.MB, r.actualCpu)) {
            // Schedule a job to a warm container
            ContainerPool
              .schedule(r.action, r.msg.user.namespace.name, freePool)
              .map(container => (container, container._2.initingState)) //warmed, warming, and warmingCold always know their state
              .orElse(
                // There was no warm/warming/warmingCold container. Try to take a prewarm container or a cold container.

                // Is there enough space to create a new container or do other containers have to be removed?
                if (hasPoolSpaceFor(busyPool ++ freePool, r.actualMemory.MB, r.actualCpu)) {
                  takePrewarmContainer(r.action)
                    .map(container => (container, "prewarmed"))
                    .orElse(Some(createContainer(r.actualMemory.MB, r.actualCpu), "cold"))
                } else None)
              .orElse(
                // Remove a container and create a new one for the given job
                ContainerPool
                // Only free up the amount, that is really needed to free up
                  .remove(
                    freePool, 
                    Math.min(r.actualMemory, memoryConsumptionOf(freePool)).MB, 
                    Math.min(r.actualCpu, cpuConsumptionOf(freePool))
                  )
                  .map(removeContainer)
                  // If the list had at least one entry, enough containers were removed to start the new container. After
                  // removing the containers, we are not interested anymore in the containers that have been removed.
                  .headOption
                  .map(_ =>
                    takePrewarmContainer(r.action)
                      .map(container => (container, "recreatedPrewarm"))
                      .getOrElse(createContainer(r.actualMemory.MB, r.actualCpu), "recreated")))

          } else None

        createdContainer match {
          case Some(((actor, data), containerState)) =>
            //increment active count before storing in pool map
            val newData = data.nextRun(r)
            val container = newData.getContainer
            logging.info(this, s"job ${r} assigned container ${container}")

            if (newData.activeActivationCount < 1) {
              logging.error(this, s"invalid activation count < 1 ${newData}")
            }

            //only move to busyPool if max reached
            if (!newData.hasCapacity()) {
              if (r.action.limits.concurrency.maxConcurrent > 1) {
                logging.info(
                  this,
                  s"container ${container} is now busy with ${newData.activeActivationCount} activations")
              }
              busyPool = busyPool + (actor -> newData)
              freePool = freePool - actor
            } else {
              //update freePool to track counts
              freePool = freePool + (actor -> newData)
            }
            // Remove the action that was just executed from the buffer and execute the next one in the queue.
            if (isResentFromBuffer) {
              // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
              // from the buffer
              val (_, newBuffer) = runBuffer.dequeue
              runBuffer = newBuffer
              // Try to process the next item in buffer (or get another message from feed, if buffer is now empty)
              processBufferOrFeed()
            }
            actor ! r // forwards the run request to the container
            logContainerStart(r, containerState, newData.activeActivationCount, container)

          case None =>
            // this can also happen if createContainer fails to start a new container, or
            // if a job is rescheduled but the container it was allocated to has not yet destroyed itself
            // (and a new container would over commit the pool)
            val isErrorLogged = r.retryLogDeadline.map(_.isOverdue).getOrElse(true)
            val retryLogDeadline = if (isErrorLogged) {
              logging.warn(
                this,
                s"Rescheduling Run message, too many message in the pool, " +
                  s"freePoolSize: ${freePool.size} containers and ${memoryConsumptionOf(freePool)} MB, " +
                  s"busyPoolSize: ${busyPool.size} containers and ${memoryConsumptionOf(busyPool)} MB, " +
                  s"maxContainersMemory ${poolConfig.userMemory.toMB} MB, " +
                  s"userNamespace: ${r.msg.user.namespace.name}, action: ${r.action}, " +
                  s"needed memory: ${r.actualMemory} MB, cpu: ${r.actualCpu}, " +
                  s"waiting messages: ${runBuffer.size}")(r.msg.transid)
              MetricEmitter.emitCounterMetric(LoggingMarkers.CONTAINER_POOL_RESCHEDULED_ACTIVATION)
              Some(logMessageInterval.fromNow)
            } else {
              r.retryLogDeadline
            }
            if (!isResentFromBuffer) {
              // Add this request to the buffer, as it is not there yet.
              runBuffer = runBuffer.enqueue(Run(r.action, r.msg, retryLogDeadline))
            }
          //buffered items will be processed via processBufferOrFeed()
        }
      } else {
        // There are currently actions waiting to be executed before this action gets executed.
        // These waiting actions were not able to free up enough memory.
        runBuffer = runBuffer.enqueue(r)
      }

    // Container is free to take more work
    case NeedWork(warmData: WarmedData) =>
      val oldData = freePool.get(sender()).getOrElse(busyPool(sender()))
      val newData =
        warmData.copy(lastUsed = oldData.lastUsed, activeActivationCount = oldData.activeActivationCount - 1)
      if (newData.activeActivationCount < 0) {
        logging.error(this, s"invalid activation count after warming < 1 ${newData}")
      }
      if (newData.hasCapacity()) {
        //remove from busy pool (may already not be there), put back into free pool (to update activation counts)
        freePool = freePool + (sender() -> newData)
        if (busyPool.contains(sender())) {
          busyPool = busyPool - sender()
          if (newData.action.limits.concurrency.maxConcurrent > 1) {
            logging.info(
              this,
              s"concurrent container ${newData.container} is no longer busy with ${newData.activeActivationCount} activations")
          }
        }
      } else {
        busyPool = busyPool + (sender() -> newData)
        freePool = freePool - sender()
      }
      processBufferOrFeed()

    // Container is prewarmed and ready to take work
    case NeedWork(data: PreWarmedData) =>
      prewarmStartingPool = prewarmStartingPool - sender()
      prewarmedPool = prewarmedPool + (sender() -> data)

    // Container got removed
    case ContainerRemoved =>
      // if container was in free pool, it may have been processing (but under capacity),
      // so there is capacity to accept another job request
      freePool.get(sender()).foreach { f =>
        freePool = freePool - sender()
      }
      // container was busy (busy indicates at full capacity), so there is capacity to accept another job request
      busyPool.get(sender()).foreach { _ =>
        busyPool = busyPool - sender()
      }
      processBufferOrFeed()

      //in case this was a prewarm
      prewarmedPool.get(sender()).foreach { _ =>
        logging.info(this, "failed prewarm removed")
        prewarmedPool = prewarmedPool - sender()
      }
      //in case this was a starting prewarm
      prewarmStartingPool.get(sender()).foreach { _ =>
        logging.info(this, "failed starting prewarm removed")
        prewarmStartingPool = prewarmStartingPool - sender()
      }

      //backfill prewarms on every ContainerRemoved, just in case
      backfillPrewarms(false) //in case a prewarm is removed due to health failure or crash
    
    // Run is completed by a children actor
    case RunCompleted(r: Run) =>
      // Preemptive release this run
      preemptiveRelease(r, isSafeguard=false)

      // Summarize to Redis
      watchdog.get(r) match { 
        case Some(b) => {
          // Ignore invoker health test actions
          if (!r.action.name.name.contains("invokerHealthTest")) {
            val (id, trajectory) = b.summary()
            redis.setTrajectoryForInvocation(id, trajectory)
          }
        }
        case None => logging.error(this, s"job ${r} no found in watchdog")
      }

      // Update record and watchdog
      record = record - r
      watchdog = watchdog - r

    case UpdateContainer(r: Run, container: Container) =>
      // Map the activation to its container
      if (record.contains(r)) {
        record = record - r
      }
      record = record + (r -> container)
      logging.info(this, s"job ${r} forwarded to container ${container}")

    // Trigger safeguard
    case Safeguard(r: Run, container: Container) =>
      // First preemptive release
      preemptiveRelease(r, isSafeguard=true)
      // Then return back (already done in container proxy)
      // safeguardUpdate(r, container)
      // Label this invocation
      watchdog.get(r).foreach{
        case b: Budget => b.isSafeguard = 1
      }

    // This message is received for one of these reasons:
    // 1. Container errored while resuming a warm container, could not process the job, and sent the job back
    // 2. The container aged, is destroying itself, and was assigned a job which it had to send back
    // 3. The container aged and is destroying itself
    // Update the free/busy lists but no message is sent to the feed since there is no change in capacity yet
    case RescheduleJob =>
      freePool = freePool - sender()
      busyPool = busyPool - sender()
    case EmitMetrics =>
      emitMetrics()
  }

  //
  // Preemptive release cpu and memory harvested from this run if any (bypass free pool due to no keep-alive time)
  //
  def preemptiveRelease(r: Run, isSafeguard: Boolean) = {
    implicit val tid = r.msg.transid
    logging.info(this, s"starting release ${r}")

    var update = immutable.Map.empty[Run, (Int, Int)] // Invocation -> (memory, cpu)

    watchdog.get(r).foreach{
      b => {
        val current: Long = Instant.now.toEpochMilli
        if (!isSafeguard) {
          // Memory borrowed return to memoryHRP
          b.memoryIn.foreach{
            case(k, v) => memoryHRP.insert(k, v, current, k.startTime + k.predictDuration)
            logging.info(this, s"memoryHRP insert ${k}, ${v}, ${current}, ${k.predictDuration}. Reharvested from job ${k}")
          }
          // CPU borrowed return to cpuHRP
          b.cpuIn.foreach{
            case(k, v) => cpuHRP.insert(k, v, current, k.startTime + k.predictDuration)
            logging.info(this, s"cpuHRP insert ${k}, ${v}, ${current}, ${k.predictDuration}. Reharvested from job ${k}")
          }
        }
        // Memory harvested release immediately
        b.memoryOut.foreach{
          case(k, v) => update = update + update.get(k).map{case (m, c) => k -> (m + v*MemoryLimit.MEM_UNIT, c)}.getOrElse(k -> (v*MemoryLimit.MEM_UNIT, 0))
          logging.info(this, s"release ${k}'s memory, ${v*MemoryLimit.MEM_UNIT}, ${current}, ${k.predictDuration}. Preemptive released from job ${k}")
        }
        // CPU harvested release immediately
        b.cpuOut.foreach{
          case(k, v) => update = update + update.get(k).map{case (m, c) => k -> (m, c + v)}.getOrElse(k -> (0, v))
          logging.info(this, s"release ${k}'s cpu, ${v}, ${current}, ${k.predictDuration}. Preemptive released from job ${k}")
        }
      }
    }

    // Update containers, bypass free pool
    update.foreach{
      case (job, (m, c)) => {
        record.get(job).foreach{
          case container: Container =>
            if (m != 0 || c != 0) {
              // Release from other containers
              val mem_user = MemoryLimit.decodeMemory(job.action.limits.memory.megabytes)
              val cpu_user = MemoryLimit.decodeCpu(job.action.limits.memory.megabytes)
              if (job.actualMemory - m < mem_user) {
                job.actualMemory = mem_user
              } else {
                job.actualMemory = job.actualMemory - m
              }
              if (job.actualCpu - c < cpu_user) {
                job.actualCpu = cpu_user
              } else {
                job.actualCpu = job.actualCpu - c
              }
              container.update(job.actualCpu, job.actualMemory.MB)
              logging.info(this, s"update job ${job}, container ${container} with cpu ${job.actualCpu} and memory ${job.actualMemory.MB}")
            }
          case _ => 
        }

        // Remove corresponding values from budget of each other container
        watchdog.get(job).foreach{
          case b: Budget => {
            b.removeMemoryIn(Vector(r))
            b.removeCpuIn(Vector(r))
            logging.info(this, s"remove job ${job} memoryIn and cpuIn from budget")
          }
        }
      }
    }

    // Release from harvested resource pools
    val memoryLeft = memoryHRP.remove(r)
    logging.info(this, s"remove job ${r} from memoryHRP")
    val cpuLeft = cpuHRP.remove(r)
    logging.info(this, s"remove job ${r} from cpuHRP")

    // Reset the out of src invocation
    watchdog.get(r).foreach{
      case b: Budget => {
        b.resetMemoryOut(memoryLeft)
        memoryLeft match {
          case Some((value, start, end)) => logging.info(this, s"record memory idle time for ${r}: dequeue time ${Instant.now.toEpochMilli}, enqueue time ${start}, with value ${value}")
          case None => logging.info(this, s"no memory left for ${r} in pool")
        }
        b.resetCpuOut(cpuLeft)
        cpuLeft match {
          case Some((value, start, end)) => logging.info(this, s"record cpu idle time for ${r}: dequeue time ${Instant.now.toEpochMilli}, enqueue time ${start}, with value ${value}")
          case None => logging.info(this, s"no cpu left for ${r} in pool")
        }
      }
    }

    logging.info(this, s"finish release ${r}")
  }

  def safeguardUpdate(r: Run, container: Container) = {
    implicit val tid = r.msg.transid
    val memory =  MemoryLimit.decodeMemory(r.action.limits.memory.megabytes)
    val cpu = MemoryLimit.decodeCpu(r.action.limits.memory.megabytes)
    container.update(cpu, memory.MB)
    logging.info(this, s"safeguard update job ${r}, container ${container} with cpu ${cpu} and memory ${memory.MB}")
  }

  /** Resend next item in the buffer, or trigger next item in the feed, if no items in the buffer. */
  def processBufferOrFeed() = {
    // If buffer has more items, and head has not already been resent, send next one, otherwise get next from feed.
    runBuffer.dequeueOption match {
      case Some((run, _)) => //run the first from buffer
        implicit val tid = run.msg.transid
        //avoid sending dupes
        if (resent.isEmpty) {
          logging.info(this, s"re-processing from buffer (${runBuffer.length} items in buffer)")
          resent = Some(run)
          self ! run
        } else {
          //do not resend the buffer head multiple times (may reach this point from multiple messages, before the buffer head is re-processed)
        }
      case None => //feed me!
        feed ! MessageFeed.Processed
    }
  }

  /** Install prewarm containers up to the configured requirements for each kind/memory combination. */
  def backfillPrewarms(init: Boolean) = {
    prewarmConfig.foreach { config =>
      val kind = config.exec.kind
      val memory = config.memoryLimit
      val currentCount = prewarmedPool.count {
        case (_, PreWarmedData(_, `kind`, `memory`, _)) => true //done starting
        case _                                          => false //started but not finished starting
      }
      val startingCount = prewarmStartingPool.count(p => p._2._1 == kind && p._2._2 == memory)
      val containerCount = currentCount + startingCount
      if (containerCount < config.count) {
        logging.info(
          this,
          s"found ${currentCount} started and ${startingCount} starting; ${if (init) "initing" else "backfilling"} ${config.count - containerCount} pre-warms to desired count: ${config.count} for kind:${config.exec.kind} mem:${config.memoryLimit.toString}")(
          TransactionId.invokerWarmup)
        (containerCount until config.count).foreach { _ =>
          prewarmContainer(config.exec, config.memoryLimit)
        }
      }
    }
  }

  /** Creates a new container and updates state accordingly. */
  def createContainer(memoryLimit: ByteSize, cpuLimit: Int): (ActorRef, ContainerData) = {
    val ref = childFactory(context)
    val data = MemoryData(memoryLimit)
    freePool = freePool + (ref -> data)
    ref -> data
  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize): Unit = {
    val newContainer = childFactory(context)
    prewarmStartingPool = prewarmStartingPool + (newContainer -> (exec.kind, memoryLimit))
    newContainer ! Start(exec, memoryLimit)
  }

  /**
   * Takes a prewarm container out of the prewarmed pool
   * iff a container with a matching kind and memory is found.
   *
   * @param action the action that holds the kind and the required memory.
   * @return the container iff found
   */
  def takePrewarmContainer(action: ExecutableWhiskAction): Option[(ActorRef, ContainerData)] = {
    val kind = action.exec.kind
    val memory = action.limits.memory.megabytes.MB
    prewarmedPool
      .find {
        case (_, PreWarmedData(_, `kind`, `memory`, _)) => true
        case _                                          => false
      }
      .map {
        case (ref, data) =>
          // Move the container to the usual pool
          freePool = freePool + (ref -> data)
          prewarmedPool = prewarmedPool - ref
          // Create a new prewarm container
          // NOTE: prewarming ignores the action code in exec, but this is dangerous as the field is accessible to the
          // factory
          prewarmContainer(action.exec, memory)
          (ref, data)
      }
  }

  /** Removes a container and updates state accordingly. */
  def removeContainer(toDelete: ActorRef) = {
    toDelete ! Remove
    freePool = freePool - toDelete
    busyPool = busyPool - toDelete
  }

  /**
   * Calculate if there is enough free memory and cpu within a given pool.
   *
   * @param pool The pool, that has to be checked, if there is enough free memory and cpu.
   * @param memory The amount of memory to check.
   * @param cpu The amount of cpu to check.
   * @return true, if there is enough space for the given amount of memory and cpu.
   */
  def hasPoolSpaceFor[A](pool: Map[A, ContainerData], memory: ByteSize, cpu: Int): Boolean = {
    val hasMemorySpace: Boolean = memoryConsumptionOf(pool) + memory.toMB <= poolConfig.userMemory.toMB
    val hasCpuSpace: Boolean = cpuConsumptionOf(pool) + cpu <= MemoryLimit.USER_CPU

    (hasMemorySpace && hasCpuSpace)
  }

  /**
   * Log metrics about pool state (buffer size, buffer memory requirements, active number, active memory, prewarm number, prewarm memory)
   */
  private def emitMetrics() = {
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_RUNBUFFER_COUNT, runBuffer.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_RUNBUFFER_SIZE,
      runBuffer.map(_.action.limits.memory.megabytes).sum)
    val containersInUse = freePool.filter(_._2.activeActivationCount > 0) ++ busyPool
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_ACTIVE_COUNT, containersInUse.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_ACTIVE_SIZE,
      containersInUse.map(_._2.memoryLimit.toMB).sum)
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_PREWARM_COUNT, prewarmedPool.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_PREWARM_SIZE,
      prewarmedPool.map(_._2.memoryLimit.toMB).sum)
    val unused = freePool.filter(_._2.activeActivationCount == 0)
    val unusedMB = unused.map(_._2.memoryLimit.toMB).sum
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_IDLES_COUNT, unused.size)
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_IDLES_SIZE, unusedMB)
  }
}

object ContainerPool {

  /**
   * Calculate the memory of a given pool.
   *
   * @param pool The pool with the containers.
   * @return The memory consumption of all containers in the pool in Megabytes.
   */
  protected[containerpool] def memoryConsumptionOf[A](pool: Map[A, ContainerData]): Long = {
    var sum: Int = 0
    pool.foreach(containerMap => 
      sum = sum + MemoryLimit.decodeMemory(containerMap._2.memoryLimit.toMB.toInt)
    )
    sum.toLong
  }

  /**
   * Calculate the cpu of a given pool.
   *
   * @param pool The pool with the containers.
   * @return The cpu consumption of all containers in the pool in Megabytes.
   */
  protected[containerpool] def cpuConsumptionOf[A](pool: Map[A, ContainerData]): Int = {
    var sum: Int = 0
    pool.foreach(containerMap => 
      sum = sum + MemoryLimit.decodeCpu(containerMap._2.memoryLimit.toMB.toInt)
    )
    sum
  }

  /**
   * Finds the best container for a given job to run on.
   *
   * Selects an arbitrary warm container from the passed pool of idle containers
   * that matches the action and the invocation namespace. The implementation uses
   * matching such that structural equality of action and the invocation namespace
   * is required.
   * Returns None iff no matching container is in the idle pool.
   * Does not consider pre-warmed containers.
   *
   * @param action the action to run
   * @param invocationNamespace the namespace, that wants to run the action
   * @param idles a map of idle containers, awaiting work
   * @return a container if one found
   */
  protected[containerpool] def schedule[A](action: ExecutableWhiskAction,
                                           invocationNamespace: EntityName,
                                           idles: Map[A, ContainerData]): Option[(A, ContainerData)] = {
    idles
      .find {
        case (_, c @ WarmedData(_, `invocationNamespace`, `action`, _, _, _)) if c.hasCapacity() => true
        case _                                                                                   => false
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                 => false
        }
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingColdData(`invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                  => false
        }
      }
  }

  /**
   * Finds the oldest previously used container to remove to make space for the job passed to run.
   * Depending on the space that has to be allocated, several containers might be removed.
   *
   * NOTE: This method is never called to remove an action that is in the pool already,
   * since this would be picked up earlier in the scheduler and the container reused.
   *
   * @param pool a map of all free containers in the pool
   * @param memory the amount of memory that has to be freed up
   * @return a list of containers to be removed iff found
   */
  @tailrec
  protected[containerpool] def remove[A](pool: Map[A, ContainerData],
                                         memory: ByteSize,
                                         cpu: Int,
                                         toRemove: List[A] = List.empty): List[A] = {
    // Try to find a Free container that does NOT have any active activations AND is initialized with any OTHER action
    val freeContainers = pool.collect {
      // Only warm containers will be removed. Prewarmed containers will stay always.
      case (ref, w: WarmedData) if w.activeActivationCount == 0 =>
        ref -> w
    }

    if (memory > 0.B && cpu > 0 && freeContainers.nonEmpty && memoryConsumptionOf(freeContainers) >= memory.toMB && cpuConsumptionOf(freeContainers) >= cpu) {
      // Remove the oldest container if:
      // - there is more memory/cpu required
      // - there are still containers that can be removed
      // - there are enough free containers that can be removed
      val (ref, data) = freeContainers.minBy(_._2.lastUsed)
      // Catch exception if remaining memory/cpu will be negative
      val remainingMemory = Try(memory - MemoryLimit.decodeMemory(data.memoryLimit.toMB.toInt).MB).getOrElse(0.B)
      val remainingCpu = Try(cpu - MemoryLimit.decodeCpu(data.memoryLimit.toMB.toInt)).getOrElse(0)
      remove(freeContainers - ref, remainingMemory, remainingCpu, toRemove ++ List(ref))
    } else {
      // If this is the first call: All containers are in use currently, or there is more memory/cpu needed than
      // containers can be removed.
      // Or, if this is one of the recursions: Enough containers are found to get the memory/cpu, that is
      // necessary. -> Abort recursion
      toRemove
    }
  }

  def props(invokerName: String,
            factory: ActorRefFactory => ActorRef,
            poolConfig: ContainerPoolConfig,
            feed: ActorRef,
            prewarmConfig: List[PrewarmingConfig] = List.empty) =
    Props(new ContainerPool(invokerName, factory, feed, prewarmConfig, poolConfig))
}

/** Contains settings needed to perform container prewarming. */
case class PrewarmingConfig(count: Int, exec: CodeExec[_], memoryLimit: ByteSize)
