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

package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent._
import java.time.Instant

// import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.management.scaladsl.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
// import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.core.loadBalancer.InvokerState.{Healthy, Offline, Unhealthy, Unresponsive}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader

import scala.annotation.tailrec
import scala.concurrent.Future
// import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.io.Source

/**
 * A loadbalancer that schedules workload based on a hashing-algorithm.
 *
 * ## Algorithm
 *
 * At first, for every namespace + action pair a hash is calculated and then an invoker is picked based on that hash
 * (`hash % numInvokers`). The determined index is the so called "home-invoker". This is the invoker where the following
 * progression will **always** start. If this invoker is healthy (see "Invoker health checking") and if there is
 * capacity on that invoker (see "Capacity checking"), the request is scheduled to it.
 *
 * If one of these prerequisites is not true, the index is incremented by a step-size. The step-sizes available are the
 * all coprime numbers smaller than the amount of invokers available (coprime, to minimize collisions while progressing
 * through the invokers). The step-size is picked by the same hash calculated above (`hash & numStepSizes`). The
 * home-invoker-index is now incremented by the step-size and the checks (healthy + capacity) are done on the invoker
 * we land on now.
 *
 * This procedure is repeated until all invokers have been checked at which point the "overload" strategy will be
 * employed, which is to choose a healthy invoker randomly. In a steadily running system, that overload means that there
 * is no capacity on any invoker left to schedule the current request to.
 *
 * If no invokers are available or if there are no healthy invokers in the system, the loadbalancer will return an error
 * stating that no invokers are available to take any work. Requests are not queued anywhere in this case.
 *
 * An example:
 * - availableInvokers: 10 (all healthy)
 * - hash: 13
 * - homeInvoker: hash % availableInvokers = 13 % 10 = 3
 * - stepSizes: 1, 3, 7 (note how 2 and 5 is not part of this because it's not coprime to 10)
 * - stepSizeIndex: hash % numStepSizes = 13 % 3 = 1 => stepSize = 3
 *
 * Progression to check the invokers: 3, 6, 9, 2, 5, 8, 1, 4, 7, 0 --> done
 *
 * This heuristic is based on the assumption, that the chance to get a warm container is the best on the home invoker
 * and degrades the more steps you make. The hashing makes sure that all loadbalancers in a cluster will always pick the
 * same home invoker and do the same progression for a given action.
 *
 * Known caveats:
 * - This assumption is not always true. For instance, two heavy workloads landing on the same invoker can override each
 *   other, which results in many cold starts due to all containers being evicted by the invoker to make space for the
 *   "other" workload respectively. Future work could be to keep a buffer of invokers last scheduled for each action and
 *   to prefer to pick that one. Then the second-last one and so forth.
 *
 * ## Capacity checking
 *
 * The maximum capacity per invoker is configured using `user-memory`, which is the maximum amount of memory of actions
 * running in parallel on that invoker.
 *
 * Spare capacity is determined by what the loadbalancer thinks it scheduled to each invoker. Upon scheduling, an entry
 * is made to update the books and a slot for each MB of the actions memory limit in a Semaphore is taken. These slots
 * are only released after the response from the invoker (active-ack) arrives **or** after the active-ack times out.
 * The Semaphore has as many slots as MBs are configured in `user-memory`.
 *
 * Known caveats:
 * - In an overload scenario, activations are queued directly to the invokers, which makes the active-ack timeout
 *   unpredictable. Timing out active-acks in that case can cause the loadbalancer to prematurely assign new load to an
 *   overloaded invoker, which can cause uneven queues.
 * - The same is true if an invoker is extraordinarily slow in processing activations. The queue on this invoker will
 *   slowly rise if it gets slow to the point of still sending pings, but handling the load so slowly, that the
 *   active-acks time out. The loadbalancer again will think there is capacity, when there is none.
 *
 * Both caveats could be solved in future work by not queueing to invoker topics on overload, but to queue on a
 * centralized overflow topic. Timing out an active-ack can then be seen as a system-error, as described in the
 * following.
 *
 * ## Invoker health checking
 *
 * Invoker health is determined via a kafka-based protocol, where each invoker pings the loadbalancer every second. If
 * no ping is seen for a defined amount of time, the invoker is considered "Offline".
 *
 * Moreover, results from all activations are inspected. If more than 3 out of the last 10 activations contained system
 * errors, the invoker is considered "Unhealthy". If an invoker is unhealty, no user workload is sent to it, but
 * test-actions are sent by the loadbalancer to check if system errors are still happening. If the
 * system-error-threshold-count in the last 10 activations falls below 3, the invoker is considered "Healthy" again.
 *
 * To summarize:
 * - "Offline": Ping missing for > 10 seconds
 * - "Unhealthy": > 3 **system-errors** in the last 10 activations, pings arriving as usual
 * - "Healthy": < 3 **system-errors** in the last 10 activations, pings arriving as usual
 *
 * ## Horizontal sharding
 *
 * Sharding is employed to avoid both loadbalancers having to share any data, because the metrics used in scheduling
 * are very fast changing.
 *
 * Horizontal sharding means, that each invoker's capacity is evenly divided between the loadbalancers. If an invoker
 * has at most 16 slots available (invoker-busy-threshold = 16), those will be divided to 8 slots for each loadbalancer
 * (if there are 2).
 *
 * If concurrent activation processing is enabled (and concurrency limit is > 1), accounting of containers and
 * concurrency capacity per container will limit the number of concurrent activations routed to the particular
 * slot at an invoker. Default max concurrency is 1.
 *
 * Known caveats:
 * - If a loadbalancer leaves or joins the cluster, all state is removed and created from scratch. Those events should
 *   not happen often.
 * - If concurrent activation processing is enabled, it only accounts for the containers that the current loadbalancer knows.
 *   So the actual number of containers launched at the invoker may be less than is counted at the loadbalancer, since
 *   the invoker may skip container launch in case there is concurrent capacity available for a container launched via
 *   some other loadbalancer.
 */
class LibraBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  override protected def emitMetrics() = {
    super.emitMetrics()
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_BLACKBOX,
      schedulingState.blackboxInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_MANAGED,
      schedulingState.managedInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Offline))
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Offline))
  }

  /** State needed for scheduling. */
  val schedulingState = ShardingContainerPoolBalancerState()(lbConfig)

  //
  // Redis client
  //
  val redisHost: String = Source.fromFile("/config.libra").getLines.toList(0).split("=")(1)
  val redisPort: Int = 6379
  val redisPassword: String = "openwhisk"
  val redisDatabase: Int = 0

  private val redis = new RedisClient(redisHost, redisPort, redisPassword, redisDatabase)
  redis.init

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[ShardingContainerPoolBalancerState.updateInvokers]] and [[ShardingContainerPoolBalancerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
  override def clusterSize: Int = schedulingState.clusterSize

  // Init load list
  var loadList: IndexedSeq[Int] = IndexedSeq[Int]()

  /** Update load list */
  def updateLoadList(numInvokers: Int): Unit = {
    // If not init, init first
    if (loadList.length < numInvokers) {
      loadList = IndexedSeq.fill[Int](numInvokers)(0)
    }

    for (i <- 0 until numInvokers) {
      activeActivationsPerInvokerFor(i) onComplete { 
        case Success(result) => loadList = loadList.updated(i, result)
        case _ =>
      }
    }
    // logging.info(this, s"current load list is ${loadList}")
  }

  /** 1. Publish a message to the loadbalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    val (invokersToUse, stepSizes) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)
    
    val actionName = action.name.name
    logging.info(this, s"Function ${actionName} starts scheduling at ${Instant.now.toEpochMilli}")

    // Fetch predict peak from Redis
    var predictPeak = redis.getPredictPeakForFunction(actionName)
    if (predictPeak == 0) {
      logging.error(this, s"getting action ${actionName} failed with predict peak 0, default unchanged")
      predictPeak = action.limits.memory.megabytes
    }

    // Determine harvest or accelerate
    val (memory, cpu) = (MemoryLimit.decodeMemory(predictPeak), MemoryLimit.decodeCpu(predictPeak))
    val (userMemory, userCpu) = (MemoryLimit.decodeMemory(action.limits.memory.megabytes), MemoryLimit.decodeCpu(action.limits.memory.megabytes))

    // Classify any invocations into three types
    // 1: both memory and cpu don't need acceleration (either harvested or descent)
    // 2: at least one needs to be accelerated
    var chosen: Option[InvokerInstanceId] = None
    // Type 1: return to JSQ scheduling
    if (userMemory >= memory && userCpu >= cpu) {
      // Update load list first
      updateLoadList(invokersToUse.size)

      chosen = if (invokersToUse.nonEmpty) {
        val invoker: Option[(InvokerInstanceId, Boolean)] = JSQBalancer.schedule(
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          invokersToUse,
          schedulingState.invokerSlots,
          action.limits.memory.megabytes,
          loadList
        )
        invoker.foreach {
          case (_, true) =>
            val metric =
              if (isBlackboxInvocation)
                LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
              else
                LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
            MetricEmitter.emitCounterMetric(metric)
          case _ =>
        }
        invoker.map(_._1)
      } else {
        None
      }
    } else { // Type 2: timeliness-aware scheduling
      // Get harvested resources from each invoker for computing coverage
      val (memorySummaryList, cpuSummaryList) = redis.getSummaryForInvoker(invokersToUse.size)
      if (memorySummaryList.size == 0 || cpuSummaryList.size == 0) {
        logging.error(this, s"Redis client failed to fetch invoker harvested resource list")
      }

      // Fetch predict duration from Redis
      val predictDuration: Long = redis.getPredictDurationForFunction(actionName) // ms
      if (predictDuration == 0.toLong) {
        logging.error(this, s"getting action ${actionName} failed with predict duration 0, default set to 0")
      }

      val startTime: Long = Instant.now.toEpochMilli
      val memoryDemand = (memory, startTime, startTime + predictDuration)
      val cpuDemand = (cpu, startTime, startTime + predictDuration)

      chosen = if (invokersToUse.nonEmpty) {
        val invoker: Option[(InvokerInstanceId, Boolean)] = LibraBalancer.schedule(
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          invokersToUse,
          schedulingState.invokerSlots,
          action.limits.memory.megabytes,
          memoryDemand,
          cpuDemand,
          memorySummaryList,
          cpuSummaryList,
          0.9
        )
        invoker.foreach {
          case (_, true) =>
            val metric =
              if (isBlackboxInvocation)
                LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
              else
                LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
            MetricEmitter.emitCounterMetric(metric)
          case _ =>
        }
        invoker.map(_._1)
      } else {
        None
      }
    }
    logging.info(this, s"Function ${actionName} ends scheduling at ${Instant.now.toEpochMilli}")

    chosen
      .map { invoker =>
        // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
        val memoryLimit = action.limits.memory
        val memoryLimitInfo = if (memoryLimit == MemoryLimit()) { "std" } else { "non-std" }
        val timeLimit = action.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
        logging.info(
          this,
          s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")
        val activationResult = setupActivation(msg, action, invoker)
        sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
      }
      .getOrElse {
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(
          this,
          s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  override val invokerPool =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    val input = entry.memoryLimit.toMB.toInt
    val memory = MemoryLimit.decodeMemory(input)
    val cpu = MemoryLimit.decodeCpu(input)

    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, memory, cpu))
  }
}

object LibraBalancer extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messagingProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.asString}", "health", maxPeek = 128),
            monitor))
      }

    }
    new LibraBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  def requiredProperties: Map[String, String] = kafkaHosts

  /** Generates a hash based on the string representation of namespace and action */
  def generateHash(namespace: EntityName, action: FullyQualifiedEntityName): Int = {
    (namespace.asString.hashCode() ^ action.asString.hashCode()).abs
  }

  /** Euclidean algorithm to determine the greatest-common-divisor */
  @tailrec
  def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

  /** Returns pairwise coprime numbers until x. Result is memoized. */
  def pairwiseCoprimeNumbersUntil(x: Int): IndexedSeq[Int] =
    (1 to x).foldLeft(IndexedSeq.empty[Int])((primes, cur) => {
      if (gcd(cur, x) == 1 && primes.forall(i => gcd(i, cur) == 1)) {
        primes :+ cur
      } else primes
    })

  /**
   * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
   * obtained, randomly picks a healthy invoker.
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param invokers a list of available invokers to search in, including their state
   * @param dispatched semaphores for each invoker to give the slots away from
   * @param slots Number of slots, that need to be acquired (e.g. memory in MB)
   * @param index the index to start from (initially should be the "homeInvoker"
   * @param step stable identifier of the entity to be scheduled
   * @return an invoker to schedule to or None of no invoker is available
   */
  def schedule(
    maxConcurrent: Int,
    fqn: FullyQualifiedEntityName,
    invokers: IndexedSeq[InvokerHealth],
    dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    slots: Int,
    memoryDemand: (Int, Long, Long),
    cpuDemand: (Int, Long, Long),
    memorySummaryList: IndexedSeq[Map[String, (Int, Long, Long)]],
    cpuSummaryList: IndexedSeq[Map[String, (Int, Long, Long)]],
    cpuWeight: Double
  )(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {
    val numInvokers = invokers.size
    val memorySlots = MemoryLimit.decodeMemory(slots)
    val cpuSlots = MemoryLimit.decodeCpu(slots)
    
    if (numInvokers > 0) {
      val coverageList = computeWeightedCoverage(
        memoryDemand,
        cpuDemand,
        memorySummaryList,
        cpuSummaryList,
        cpuWeight
      )
      
      coverageList.zipWithIndex.sortBy(-_._1).map{
        case (_, index) => {
          val invoker = invokers(index)
          //test this invoker - if this action supports concurrency, use the scheduleConcurrent function
          if (invoker.status.isUsable && dispatched(invoker.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, memorySlots, cpuSlots)) {
            return Some(invoker.id, false)
          }
        }
      }

      // If still not scheduled, randomly choose a healthy Invoker
      val healthyInvokers = invokers.filter(_.status.isUsable)
      if (healthyInvokers.nonEmpty) {
        // Choose a healthy invoker randomly
        val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
        dispatched(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, memorySlots, cpuSlots)
        logging.warn(this, s"system is overloaded. Chose invoker${random.toInt} by random assignment.")
        Some(random, true)
      } else {
        None
      }
    } else {
      None
    }
  }

  /** Compute weighted resource coverage for a group of invokers, given prediction of current invocation */
  def computeWeightedCoverage(
    memoryDemand: (Int, Long, Long),
    cpuDemand: (Int, Long, Long),
    memorySummaryList: IndexedSeq[Map[String, (Int, Long, Long)]],
    cpuSummaryList: IndexedSeq[Map[String, (Int, Long, Long)]],
    cpuWeight: Double
  ): IndexedSeq[Double] = {
    var memoryCoverageList: IndexedSeq[Double] = IndexedSeq[Double]()
    var cpuCoverageList: IndexedSeq[Double] = IndexedSeq[Double]()
    var weightedCoverageList: IndexedSeq[Double] = IndexedSeq[Double]()

    //
    // Compute memory coverage
    //

    memorySummaryList.map{
      case memorySummary => {
        var memoryTimestampList = Vector.empty[Long]
        var memoryHistogramMap = Map.empty[(Long, Long), Int]

        memorySummary.map{
          case(key, (value, start, end)) => {
            if (!(end <= memoryDemand._2 || start >= memoryDemand._3)) {
              if (!memoryTimestampList.contains(start) && start > memoryDemand._2) {
                memoryTimestampList = memoryTimestampList :+ start
              }
              if (!memoryTimestampList.contains(end) && end < memoryDemand._3) {
                memoryTimestampList = memoryTimestampList :+ end
              }
            }
          }
        }
        memoryTimestampList = memoryTimestampList :+ memoryDemand._2
        memoryTimestampList = memoryTimestampList :+ memoryDemand._3
        memoryTimestampList = memoryTimestampList.sorted

        memoryTimestampList.sliding(2).foreach{k => memoryHistogramMap = memoryHistogramMap + ((k(0), k(1)) -> 0)}

        memorySummary.map{
          case(key, (value, start, end)) => {
            var intersected = Vector.empty[Long]
            memoryTimestampList.map{
              case v => {
                if (start <= v && v <= end) {
                  intersected = intersected :+ v
                }
              }
            }
            // val intersected = memoryTimestampList.intersect(start to end)
            if (intersected.size > 1) {
              intersected.sliding(2).foreach{
                k => memoryHistogramMap.get((k(0), k(1))) match {
                  case Some(v) =>  {
                    if ((v + value) < memoryDemand._1) {
                      memoryHistogramMap = memoryHistogramMap + ((k(0), k(1)) -> (v + value))
                    } else {
                      memoryHistogramMap = memoryHistogramMap + ((k(0), k(1)) -> memoryDemand._1)
                    }
                  }
                  case None =>
                }
              }
            }
          }
        }
        var memoryCover: Double = 0.0
        memoryHistogramMap.map{
          case ((k1, k2), value) => {
            memoryCover = memoryCover + (k2 - k1).toDouble * value
          }
        }

        val memoryCoverage = (memoryDemand._3 - memoryDemand._2).toDouble * memoryDemand._1 / memoryCover
        memoryCoverageList = memoryCoverageList :+ memoryCoverage
      }
    }

    //
    // Compute cpu coverage
    //
    
    cpuSummaryList.map{
      case cpuSummary => {
        var cpuTimestampList = Vector.empty[Long]
        var cpuHistogramMap = Map.empty[(Long, Long), Int]

        cpuSummary.map{
          case(key, (value, start, end)) => {
            if (!(end <= cpuDemand._2 || start >= cpuDemand._3)) {
              if (!cpuTimestampList.contains(start) && start > cpuDemand._2) {
                cpuTimestampList = cpuTimestampList :+ start
              }
              if (!cpuTimestampList.contains(end) && end < cpuDemand._3) {
                cpuTimestampList = cpuTimestampList :+ end
              }
            }
          }
        }
        cpuTimestampList = cpuTimestampList :+ cpuDemand._2
        cpuTimestampList = cpuTimestampList :+ cpuDemand._3
        cpuTimestampList = cpuTimestampList.sorted

        cpuTimestampList.sliding(2).foreach{k => cpuHistogramMap = cpuHistogramMap + ((k(0), k(1)) -> 0)}

        cpuSummary.map{
          case(key, (value, start, end)) => {
            var intersected = Vector.empty[Long]
            cpuTimestampList.map{
              case v => {
                if (start <= v && v <= end) {
                  intersected = intersected :+ v
                }
              }
            }
            // val intersected = cpuTimestampList.intersect(start to end)
            if (intersected.size > 1) {
              intersected.sliding(2).foreach{
                k => cpuHistogramMap.get((k(0), k(1))) match {
                  case Some(v) =>  {
                    if ((v + value) < cpuDemand._1) {
                      cpuHistogramMap = cpuHistogramMap + ((k(0), k(1)) -> (v + value))
                    } else {
                      cpuHistogramMap = cpuHistogramMap + ((k(0), k(1)) -> cpuDemand._1)
                    }
                  }
                  case None =>
                }
              }
            }
          }
        }
        var cpuCover: Double = 0.0
        cpuHistogramMap.map{
          case ((k1, k2), value) => {
            cpuCover = cpuCover + (k2 - k1).toDouble * value
          }
        }

        val cpuCoverage = (cpuDemand._3 - cpuDemand._2).toDouble * cpuDemand._1 / cpuCover
        cpuCoverageList = cpuCoverageList :+ cpuCoverage
      }
    }

    //
    // Apply weights
    //

    memoryCoverageList.zip(cpuCoverageList).map{
      case (memoryCoverage, cpuCoverage) => {
        val weightedCoverage = (1 - cpuWeight) * memoryCoverage + cpuWeight * cpuCoverage
        weightedCoverageList = weightedCoverageList :+ weightedCoverage
      }
    }

    weightedCoverageList
  }
}
