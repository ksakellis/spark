/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.scheduler

import java.io.NotSerializableException
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark._
import org.apache.spark.rdd.{HadoopPartition, RDD}
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.apache.spark.util.CallSite
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map, Stack}
import scala.tools.scalap.scalax.rules.scalasig.Children
import scala.util.control.NonFatal

import org.apache.spark.broadcast.Broadcast

private[spark] class ExecutionPlan(
    private[scheduler] val sc: SparkContext)
  extends Logging {

  private val shuffleToMapStage = new HashMap[Int, Stage]
  private val nextStageId = new AtomicInteger(0)
  private val stageIdToStage = new HashMap[Int, Stage]
  private val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private val waitingStages = new HashSet[Stage]
  private val activeJobs = new HashSet[ActiveJob]
  private val jobIdToActiveJob = new HashMap[Int, ActiveJob]
  private val stageIdToTaskBytes = new HashMap[Int, Int]
  private val stageIdToTaskSet = new HashMap[Int, TaskSet]

  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  // Stages we are running right now
  private val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private val failedStages = new HashSet[Stage]

  private val allStages = new HashSet[Stage]

  private val WIDTH = 2

  def explainJob[T, U](
      rdd : RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null): Seq[Stage] = {

    val jobId = 1
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val listener = new FakeListener()
    handleJobSubmitted(jobId, rdd, func2, partitions.toArray, callSite, listener, properties)
    val allStages = runningStages.toSeq ++ waitingStages.toSeq

    while(!runningStages.isEmpty) {
      runningStages.foreach({ stage =>
        stage.pendingTasks.foreach({ task =>
          val event = new CompletionEvent(task, Success, null, Map[Long, Any](), null, null)
          handleTaskCompletion(event)
        })
      })
    }

    val dag = DAG.apply(allStages.last)

    println(dag.toInfoString(WIDTH))

    allStages

  }

  abstract class Node(val id : Int) extends Logging {

    val adjacentNodes = new HashSet[Node]
    val parentNodes = new HashSet[Node]

    def addAdjacentNode(node : Node) : Unit = {
      adjacentNodes.add(node)
      node.parentNodes.add(this)
    }

    def visit(visitor : (Node) => Unit, forward : Boolean) {
      visitor(this)
      val set = if (forward) adjacentNodes else parentNodes
      set.foreach(_.visit(visitor, forward))
    }

    def toInfoString(width : Int): String
  }

  class RddNode(val rdd : RDD[_]) extends Node(rdd.id) {

    override def toInfoString(width : Int): String = {

      def padded(str : String) : String = (" " * (width + 1)) + str

      var idStr = (" " * width) + "RDD " + id.toString +
        ":  [" + rdd.getClass.getSimpleName + "] " +
        "(" + rdd.getCreationSite + ")"
     /*

      val info = Map(
        "DependencyOf" -> adjacentNodes.map(_.id).toSeq,
      )

      info.foreach({ entry =>
        idStr += "\n" + padded(entry._1 + ": " + entry._2.mkString(","))
      })
      */

      idStr
    }
  }

  class TaskNode(val task : Task[_]) {
    val taskType = task.getClass.getSimpleName
    val partitionId = task.partitionId
    var partitionString = task match {
        case x: ShuffleMapTask => {
            var str = "Type: " + x.partition.getClass.getSimpleName
            x.partition match {
              case y: HadoopPartition => {
                val split = y.inputSplit
                str += " Size: " + split.t.getLength() +
                " Locations: [" + split.t.getLocations.mkString(", ") + "]"
              }
              case y => ""
            }
            str
        }
        case x => ""
      }

  }
  

  class StageNode(val stage : Stage) extends Node(stage.id) {

    val boxSize = 5
    val numTasks = stage.numTasks
    val numPartitions = stage.numPartitions
    val rddDAG = buildRddDAG(stage.rdd)
    val tasks = buildTasks(stageIdToTaskSet.get(id).get)
    val callSite = stage.callSite
    
    def buildTasks(tasks : TaskSet): HashSet[TaskNode] = {
      val set = new HashSet[TaskNode]
      tasks.tasks.foreach({t =>
        set.add(new TaskNode(t))
      })
      set
    }
    
    def buildRddDAG(finalRdd : RDD[_]) : DAG = {
      val seenRdds = new HashMap[Int, RddNode]

      def buildNode(rdd : RDD[_]) : RddNode = {
        val node = seenRdds.getOrElseUpdate(rdd.id, new RddNode(rdd))
        val deps = rdd.dependencies
        rdd.deps.foreach({dep =>
          val parentNode = buildNode(dep.rdd)
          parentNode.addAdjacentNode(node)
        })
        node
      }
      val fNode = buildNode(finalRdd)
      val f = new HashSet[RddNode]
      fNode.visit({node : Node =>
        if (node.parentNodes.isEmpty) {
          f.add(node.asInstanceOf[RddNode])
        }
      }, false)

      new DAG(f.toSeq, fNode)
    }

    override def toInfoString(width : Int): String = {

      def padded(str : String) : String = (" " * (width + 1)) + str

      var idStr = "Stage " + id.toString + ":"
      val info = Map(
        "ParentOf" -> adjacentNodes.map(_.id).toSeq,
        //"NumTasks" -> Seq(numTasks),
        //"NumPartitions" -> Seq(numPartitions),
        "Callsite" -> Seq(callSite.shortForm),
        "TaskBinarySize" -> Seq(stageIdToTaskBytes.get(id).get + " bytes")
      )

      info.foreach({ entry =>
        idStr += "\n" + padded(entry._1 + ": " + entry._2.mkString(","))
      })
      idStr += "\n"

      idStr += padded("RDD Chain: ")
      idStr += rddDAG.toInfoString(width + 2)

      idStr += padded("Tasks")
      idStr += tasksInfoString(width + 2)

      idStr
    }

    def tasksInfoString(width : Int): String = {
      def padded(str : String) : String = (" " * (width + 1)) + str

      var idStr = ""
      tasks.foreach({task =>
        idStr += "\n"
        idStr += (" " * width) + "TaskPartition " + task.partitionId +
          ":  [" + task.taskType + "]"
        val info = Map(
          "Partition" -> task.partitionString
        )
        info.foreach({ entry =>
          idStr += "\n" + padded(entry._1 + ": " + entry._2)
        })
        idStr += "\n"
      })
      idStr
    }
  }

  class DAG(val roots : Seq[Node],
            val lastStage : Node) {

    class Level(val nodes : Seq[Node]) {
      val numNodes = nodes.size
    }

    def toInfoString(width : Int) : String = {
      val queue = new mutable.Queue[Level]()
      queue.enqueue(new Level(roots))

      var str = ""
      while(!queue.isEmpty) {
        str += "\n"
        val levelItems = new HashSet[Node]
        val level = queue.dequeue()
        level.nodes.foreach({item =>
          str += item.toInfoString(width) + "\n"
          item.adjacentNodes.foreach({node => levelItems.add(node)})
        })
        if (levelItems.size > 0) {
          queue.enqueue(new Level(levelItems.toSeq))
        }
      }
      str + "\n"
    }

  }


  object DAG {
    def apply(finalStage : Stage) = {
      val seenStages = new HashMap[Int, StageNode]

      def buildStageNode(stage : Stage) : StageNode = {
        val node = seenStages.getOrElseUpdate(stage.id, new StageNode(stage))
        stage.parents.foreach({ parent =>
          val parentNode = buildStageNode(parent)
          parentNode.addAdjacentNode(node)
        })
        node
      }
      val fStageNode = buildStageNode(finalStage)
      val f = new HashSet[StageNode]
      fStageNode.visit({node : Node =>
        if (node.parentNodes.isEmpty) {
          f.add(node.asInstanceOf[StageNode])
        }
      }, false)

      new DAG(f.toSeq, fStageNode)
    }
  }

  private class FakeListener extends JobListener {
    def taskSucceeded(index: Int, result: Any) = {}
    def jobFailed(exception: Exception) = {}
  }

  // Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet
  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                parents.push(shufDep)
              }

              waitingForVisit.push(shufDep.rdd)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }

    waitingForVisit.push(rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }


  // Find ancestor missing shuffle dependencies and register into shuffleToMapStage
  private def registerShuffleDependencies(shuffleDep: ShuffleDependency[_, _, _], jobId: Int) = {
    val parentsWithNoMapStage = getAncestorShuffleDependencies(shuffleDep.rdd)
    while (!parentsWithNoMapStage.isEmpty) {
      val currentShufDep = parentsWithNoMapStage.pop()
      val stage =
        newOrUsedStage(
          currentShufDep.rdd, currentShufDep.rdd.partitions.size, currentShufDep, jobId,
          currentShufDep.rdd.creationSite)
      shuffleToMapStage(currentShufDep.shuffleId) = stage
    }
  }

  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): Stage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        // We are going to register ancestor shuffle dependencies
        registerShuffleDependencies(shuffleDep, jobId)
        // Then register current shuffleDep
        val stage =
          newOrUsedStage(
            shuffleDep.rdd, shuffleDep.rdd.partitions.size, shuffleDep, jobId,
            shuffleDep.rdd.creationSite)
        shuffleToMapStage(shuffleDep.shuffleId) = stage

        stage
    }
  }


  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        ///if (getCacheLocs(rdd).contains(Nil)) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.jobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
      //  }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, jobId)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    parents.toList
  }


  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage) {
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parents: List[Stage] = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }


  private def newStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: Option[ShuffleDependency[_, _, _]],
      jobId: Int,
      callSite: CallSite)
    : Stage =
  {
    val parentStages = getParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id, rdd, numTasks, shuffleDep, parentStages, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }


  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(failedStage: Stage, reason: String) {
    //logError("IN ABORT for stage " + failedStage.id + ": " + reason)

  }



  private def newOrUsedStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      jobId: Int,
      callSite: CallSite)
    : Stage =
  {
    val stage = newStage(rdd, numTasks, Some(shuffleDep), jobId, callSite)
   /*
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.size) {
        stage.outputLocs(i) = Option(locs(i)).toList   // locs(i) will be null if missing
      }
      stage.numAvailableOutputs = locs.count(_ != null)
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.size)
    }
    */
    stage
  }

  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      //logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        //logDebug("missing: " + missing)
        if (missing == Nil) {
         // logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id)
    }
  }


  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    //logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingTasks.clear()

    // First figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = {
      if (stage.isShuffleMap) {
        (0 until stage.numPartitions).filter(id => stage.outputLocs(id) == Nil)
      } else {
        val job = stage.resultOfJob.get
        (0 until job.numPartitions).filter(id => !job.finished(id))
      }
    }

    val properties = if (jobIdToActiveJob.contains(jobId)) {
      jobIdToActiveJob(stage.jobId).properties
    } else {
      // this stage will be assigned to "default" pool
      null
    }

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage.latestInfo = StageInfo.fromStage(stage, Some(partitionsToCompute.size))
    //listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] =
        if (stage.isShuffleMap) {
          closureSerializer.serialize((stage.rdd, stage.shuffleDep.get) : AnyRef).array()
        } else {
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func) : AnyRef).array()
        }
      taskBinary = sc.broadcast(taskBinaryBytes)
      stageIdToTaskBytes.put(stage.id, taskBinaryBytes.length)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString)
        runningStages -= stage
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}")
        runningStages -= stage
        return
    }

    val tasks: Seq[Task[_]] = if (stage.isShuffleMap) {
      partitionsToCompute.map { id =>
        val locs = getPreferredLocs(stage.rdd, id)
        val part = stage.rdd.partitions(id)
        new ShuffleMapTask(stage.id, taskBinary, part, locs)
      }
    } else {
      val job = stage.resultOfJob.get
      partitionsToCompute.map { id =>
        val p: Int = job.partitions(id)
        val part = stage.rdd.partitions(p)
        val locs = getPreferredLocs(stage.rdd, p)
        new ResultTask(stage.id, taskBinary, part, locs, id)
      }
    }

    if (tasks.size > 0) {
      // Preemptively serialize a task to make sure it can be serialized. We are catching this
      // exception here because it would be fairly hard to catch the non-serializable exception
      // down the road, where we have several different implementations for local scheduler and
      // cluster schedulers.
      //
      // We've already serialized RDDs and closures in taskBinary, but here we check for all other
      // objects such as Partition.
      try {
        closureSerializer.serialize(tasks.head)
      } catch {
        case e: NotSerializableException =>
          abortStage(stage, "Task not serializable: " + e.toString)
          runningStages -= stage
          return
        case NonFatal(e) => // Other exceptions, such as IllegalArgumentException from Kryo.
          abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}")
          runningStages -= stage
          return
      }

      stage.pendingTasks ++= tasks
      //logDebug("New pending tasks: " + stage.pendingTasks)
      val taskset = new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(),
        stage.jobId, properties)
      //stage.previousTaskSets.add(taskset)
      stageIdToTaskSet.put(stage.id, taskset)

      //taskScheduler.submitTasks(
      //  new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))

      //stage.latestInfo.submissionTime = Some(clock.getTime())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should post
      // SparkListenerStageCompleted here in case there are no tasks to run.
      //listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
      //logDebug("Stage " + stage + " is actually done; %b %d %d".format(
      //  stage.isAvailable, stage.numAvailableOutputs, stage.numPartitions))
      runningStages -= stage
    }
  }

  private def cleanupStateForJobAndIndependentStages(job: ActiveJob) {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  runningStages -= stage
                }
                for ((k, v) <- shuffleToMapStage.find(_._2 == stage)) {
                  shuffleToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId

            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage.resultOfJob = None
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val stageId = task.stageId



    val stage = stageIdToStage(task.stageId)

    def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None) = {
      runningStages -= stage
    }

    event.reason match {
      case Success =>
        if (event.accumUpdates != null) {
          try {
            Accumulators.add(event.accumUpdates)
            event.accumUpdates.foreach { case (id, partialValue) =>
              val acc = Accumulators.originals(id).asInstanceOf[Accumulable[Any, Any]]
              // To avoid UI cruft, ignore cases where value wasn't updated
              if (acc.name.isDefined && partialValue != acc.zero) {
                val name = acc.name.get
                val stringPartialValue = Accumulators.stringifyPartialValue(partialValue)
                val stringValue = Accumulators.stringifyValue(acc.value)
                stage.latestInfo.accumulables(id) = AccumulableInfo(id, name, stringValue)
                event.taskInfo.accumulables +=
                  AccumulableInfo(id, name, Some(stringPartialValue), stringValue)
              }
            }
          } catch {
            // If we see an exception during accumulator update, just log the error and move on.
            case e: Exception =>
              logError(s"Failed to update accumulators for $task", e)
          }
        }
        stage.pendingTasks -= task
        task match {
          case rt: ResultTask[_, _] =>
            stage.resultOfJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(stage)
                    cleanupStateForJobAndIndependentStages(job)
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the stage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
            }

          case smt: ShuffleMapTask =>
            val status = event.result.asInstanceOf[MapStatus]
            val execId = -1000 //status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            stage.addOutputLoc(smt.partitionId, status)
            if (runningStages.contains(stage) && stage.pendingTasks.isEmpty) {
              markStageAsFinished(stage)
              if (stage.shuffleDep.isDefined) {
                // We supply true to increment the epoch number here in case this is a
                // recomputation of the map outputs. In that case, some nodes may have cached
                // locations with holes (from when we detected the error) and will need the
                // epoch incremented to refetch them.
                // TODO: Only increment the epoch number if this is not the first time
                //       we registered these map outputs.
                //mapOutputTracker.registerMapOutputs(
               //   stage.shuffleDep.get.shuffleId,
               //   stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray,
               //   changeEpoch = true)
              }
              //clearCacheLocs()
              if (stage.outputLocs.exists(_ == Nil)) {
                // Some tasks had failed; let's resubmit this stage
                // TODO: Lower-level scheduler should also deal with this
                submitStage(stage)
              } else {
                val newlyRunnable = new ArrayBuffer[Stage]
                for (stage <- waitingStages if getMissingParentStages(stage) == Nil) {
                  newlyRunnable += stage
                }
                waitingStages --= newlyRunnable
                runningStages ++= newlyRunnable
                for {
                  stage <- newlyRunnable.sortBy(_.id)
                  jobId <- activeJobForStage(stage)
                } {
                  submitMissingTasks(stage, jobId)
                }
              }
            }
          }
    }
    submitWaitingStages()
  }


  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = synchronized {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /** Recursive implementation for getPreferredLocs. */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_],Int)])
    : Seq[TaskLocation] =
  {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd,partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
   // val cached = getCacheLocs(rdd)(partition)
   // if (!cached.isEmpty) {
   //   return cached
   // }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (!rddPrefs.isEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }
    // If the RDD has narrow dependencies, pick the first partition of the first narrow dep
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }
      case _ =>
    }
    Nil
  }


  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      //allowLocal: Boolean,
      callSite: CallSite,
      listener: JobListener,
      properties: Properties = null)
  {
    var finalStage: Stage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = newStage(finalRDD, partitions.size, None, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    if (finalStage != null) {
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
     // clearCacheLocs()
     // val shouldRunLocally =
     //localExecutionEnabled && allowLocal && finalStage.parents.isEmpty && partitions.length == 1
    //  if (shouldRunLocally) {
        // Compute very short actions like first() or take() with no parent stages locally.
    //    listenerBus.post(SparkListenerJobStart(job.jobId, Array[Int](), properties))
    //    runLocally(job)
    //  } else {
        jobIdToActiveJob(jobId) = job
        activeJobs += job
        finalStage.resultOfJob = Some(job)
     //   listenerBus.post(SparkListenerJobStart(job.jobId, jobIdToStageIds(jobId).toArray,
     //     properties))
        submitStage(finalStage)
      //}
    }
    submitWaitingStages()
  }

  private def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    //logTrace("Checking for newly runnable parent stages")
    //logTrace("running: " + runningStages)
    //logTrace("waiting: " + waitingStages)
    //logTrace("failed: " + failedStages)
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear()
    for (stage <- waitingStagesCopy.sortBy(_.jobId)) {
      submitStage(stage)
    }
  }


}
