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

import scala.collection.immutable

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.{HadoopPartition, RDD}
import org.apache.spark.scheduler.{ShuffleMapTask, ResultTask, TaskSet, Stage, Task}
import org.apache.spark.util.CallSite
import scala.collection.mutable.{Map, ListBuffer}
import scala.util.control.Breaks

class ExecutionPlan(val graph: DAG[_]) {

  override def toString: String = {
    ExecutionPlanPrinter.generateString(graph.asInstanceOf[DAG[StageMetadata]])
  }
}

object ExecutionPlan {
  def apply(finalStage: Stage, stageIdToTaskSet: Map[Int, TaskSet]): ExecutionPlan = {
    val dag = DAG(finalStage, new DAG.Adapter[Stage, StageMetadata] {
      def id(r: Stage): Int = r.id
      def metadata(r: Stage): StageMetadata = StageMetadata(r, stageIdToTaskSet.get(r.id))
      def parents(r: Stage): Seq[Stage] = r.parents
    })
    new ExecutionPlan(dag)
  }
}

class StageMetadata(val numTasks: Int,
                    val numPartitions: Int,
                    val callSite: CallSite,
                    val rddGraph: DAG[RddMetadata],
                    val tasks: Seq[TaskMetadata]) {}

object StageMetadata {

  def apply(stage: Stage, taskSet: Option[TaskSet]): StageMetadata = {
    new StageMetadata(stage.numTasks, stage.numPartitions,
      stage.callSite, buildRddGraph(stage.rdd), buildTasks(taskSet))
  }

  def buildRddGraph(rdd: RDD[_]): DAG[RddMetadata] = {
    DAG(rdd, new DAG.Adapter[RDD[_], RddMetadata] {
      def id(r: RDD[_]): Int = r.id
      def metadata(r: RDD[_]): RddMetadata = new RddMetadata(r)
      def parents(r: RDD[_]): Seq[RDD[_]] = {

        val mybreaks = new Breaks
        import mybreaks.{break, breakable}

        val dependencies = r.dependencies
        val rdds = new ListBuffer[RDD[_]]
        breakable {
          dependencies.foreach(dep => {
            dep match {
              case d: ShuffleDependency[_,_,_] =>
                break
              case _ =>
                rdds += dep.rdd
            }
          })
        }
        rdds
      }
    })
  }

  def buildTasks(taskSet: Option[TaskSet]): Seq[TaskMetadata] = {
    taskSet.map(set => set.tasks.map(task => new TaskMetadata(task))).get.toSeq
  }
}

class TaskMetadata(val task: Task[_]) {
  val taskType = task.getClass.getSimpleName
  val partitionId = task.partitionId
  val taskBinary = (task match {
    case t: ShuffleMapTask => t.taskBinary
    case t: ResultTask[_,_] => t.taskBinary
  }).value
  def taskBinarySize = taskBinary.size

  val partition = task match {
    case t: ShuffleMapTask => t.partition
    case t: ResultTask[_,_] => t.partition
  }
  def partitionType = partition.getClass.getSimpleName

  val extraPartitionInfo = partition match {
    case p: HadoopPartition => {
      val split = p.inputSplit
      immutable.Map(
        "Size" -> split.t.getLength,
        "Locations" -> split.t.getLocations.mkString(", ")
      )
    }
    case p => immutable.Map()
  }
}

class RddMetadata(val rdd: RDD[_]) {}

