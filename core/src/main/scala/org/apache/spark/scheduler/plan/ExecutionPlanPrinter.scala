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

import org.apache.spark.util.Utils

object ExecutionPlanPrinter {

  final val stageMargin: Int = 1

  class StageInfo(val stageVertex: Vertex[StageMetadata]) {
    override def toString: String = {
      val meta = stageVertex.metadata
      s"""
       |Stage: ${stageVertex.id} @ ${meta.callSite.shortForm.split("at")(0)}
       |Tasks: ${meta.numTasks}
      """.stripMargin
    }
  }

  class RddInfo(val rddVertex: Vertex[RddMetadata]) {
    override def toString: String = {
      val rdd = rddVertex.metadata.rdd
      s"""
       |RDD: ${rdd.id} @ ${rdd.getCreationSite.split("at")(0)}
       |${rdd.getClass.getSimpleName}
      """.stripMargin
    }
  }

  def generateString(thegraph: DAG[StageMetadata]): String = {
    val writer = new ConsoleWriter

    writer.writeLineBuffer()
    writer.writeRaw(thegraph.toString(new StageInfo(_)))
          .writeLineBuffer()
          .writeln("STAGE DETAILS:")
          .writeln("--------------")
          .writeln

    thegraph.foreach(v => {
      val meta = v.metadata

      writer.writeln(s"Stage: ${v.id}")
            .addMargin(stageMargin)
            .writeln(s"Callsite: ${meta.callSite.shortForm}")

      meta.tasks.foreach(t => {
        writer.writeln
        taskString(t, writer)
        writer.writeln
      })
      writer.writeln
            .writeln("RDD Chain:")
            .writeRaw(meta.rddGraph.toString(new RddInfo(_)))
            .writeLineBuffer()
            .removeMargin(stageMargin)
    })
    writer.getString
  }

  def taskString(metadata: TaskMetadata, writer: ConsoleWriter): Unit = {
    writer.writeln(metadata.taskType)
          .addMargin(1)
          .write(s"PartitionId: ${metadata.partitionId} ")
          .writeRaw(s"Type: ${metadata.partitionType} ")

    metadata.extraPartitionInfo.foreach { e =>
      writer.writeRaw(s"${e._1}: ${e._2} ")
    }
    writer.writeln
    val binarySize = Utils.bytesToString(metadata.taskBinarySize)
    writer.write(s"Binary Size: $binarySize")
    writer.removeMargin(1)
  }

  class ConsoleWriter {
    def padded(str : String, width: Int) : String = (" " * (width + 1)) + str

    val builder = new StringBuilder
    var currentMargin = 0
    def setMargin(margin: Int): ConsoleWriter = {
      currentMargin = margin
      this
    }
    def addMargin(delta: Int): ConsoleWriter = setMargin(currentMargin + delta)
    def removeMargin(delta: Int): ConsoleWriter = setMargin(currentMargin - delta)
    def writeln: ConsoleWriter = writeln("")
    def writeln(line: String, extraPadding: Int = 0): ConsoleWriter = {
      write(line)
      write("\n")
    }
    def writeRaw(text: String): ConsoleWriter = {
      builder.append(text)
      this
    }
    def write(text: String, extraPadding: Int = 0): ConsoleWriter = {
      writeRaw(padded(text, currentMargin + extraPadding))
    }
    def writeLineBuffer(): ConsoleWriter = {
      writeln
      writeln
    }
    def getString = builder.toString()
  }
}
