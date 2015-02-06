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
package org.apache.spark.scheduler.plan

import java.util.Properties

import org.apache.commons.lang.math.RandomUtils
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

import scala.collection.mutable.{ListBuffer, HashMap, Map}
import scala.reflect.ClassTag

private[spark] class ExecutionPlanner(
    private[scheduler] val sc: SparkContext)
  extends Logging {

  private val taskScheduler = new NopTaskScheduler
  private val dagScheduler = new DAGScheduler(sc, taskScheduler)

  def createExecutionPlan[T, U: ClassTag](rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    callSite: CallSite,
    resultHandler: (Int, U) => Unit,
    properties: Properties = null): ExecutionPlan = {

    val allowLocal = true
    val jobId = 1
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]

    val waiter = new NopJobWaiter(partitions.size)
    dagScheduler.eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, allowLocal, callSite, waiter, properties))
    waiter.awaitFinish()
   
    ExecutionPlan(taskScheduler.allStages.last, taskScheduler.stageIdToTaskSet)
  }
}


class NopJobWaiter(val totalTasks: Int) extends JobListener {

  private var finishedTasks = 0

  @volatile
  private var _jobFinished = totalTasks == 0

  def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    finishedTasks += 1
    if (finishedTasks == totalTasks) {
      _jobFinished = true
      this.notifyAll()
    }
  }

  def jobFailed(exception: Exception): Unit = synchronized {
    _jobFinished = true
    this.notifyAll()
  }

  def awaitFinish(): Unit = synchronized {
    while (!_jobFinished) {
      this.wait()
    }
  }
}

class NopTaskScheduler extends TaskScheduler {

  val allStages = new ListBuffer[Stage]
  val stageIdToTaskSet = new HashMap[Int, TaskSet]
  var dagScheduler: DAGScheduler = null

  override def rootPool: Pool = null

  override def stop(): Unit = {}

  override def schedulingMode: SchedulingMode = SchedulingMode.FIFO

  override def defaultParallelism(): Int = 1

  override def submitTasks(taskSet: TaskSet): Unit = {
    stageIdToTaskSet.put(taskSet.stageId, taskSet)
    allStages += dagScheduler.stageIdToStage(taskSet.stageId)
    taskSet.tasks.foreach({ task =>
      val result = MapStatus(BlockManagerId(s"${RandomUtils.nextInt}", "myHost",
        1024 + RandomUtils.nextInt(7000)), Array(1L, 2L, 3L))
      val event = CompletionEvent(task, Success, result, Map[Long, Any](), null, null)
      dagScheduler.eventProcessLoop.post(event)
    })
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {}

  override def executorHeartbeatReceived(execId: String, taskMetrics: Array[(Long, TaskMetrics)],
    blockManagerId: BlockManagerId): Boolean = true

  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler
  }

  override def start(): Unit = {}
}
