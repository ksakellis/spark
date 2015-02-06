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

import scala.collection.mutable

import com.github.mdr.ascii.layout.{Graph => DrawGraph, Layouter}

private[spark] class DAG[T](val roots: mutable.HashMap[Int, Vertex[T]])
  extends Iterable[Vertex[T]] {

  def rootVertices: Seq[Vertex[T]] = roots.map(_._2).toSeq

  def adjacencyList: Seq[(Vertex[T], Vertex[T])] = {
    val edgelist = new mutable.HashSet[(Vertex[T], Vertex[T])]
    rootVertices.foreach(_.visit({v: Vertex[T] =>
      val children = v.children.values
      children.foreach(n => {
        val t = v -> n
        edgelist.add(t)
      })
    }))
    edgelist.toSeq
  }

  def verticies: Seq[Vertex[T]] = {
    val verticies = new mutable.HashSet[(Vertex[T])]
    verticies ++= rootVertices
    adjacencyList.foreach(tuple => {
      verticies.add(tuple._1)
      verticies.add(tuple._2)
    })
    verticies.toSeq
  }

  override def iterator: Iterator[Vertex[T]] = {
    // This is really lame. Need to make a real iterator
    new Iterator[Vertex[T]] {
      val verts = verticies.iterator
      override def hasNext: Boolean = {
        verts.hasNext
      }
      override def next: Vertex[T] = {
        verts.next()
      }
    }
  }

  def toString(mapper: (Vertex[T]) => Any): String = {
    val remap = verticies.map(i => i -> mapper(i)).toMap

    val vertexList = remap.values.toList
    val adjList = adjacencyList.map(e => (remap.get(e._1).get, remap.get(e._2).get)).toList

    val graph = DrawGraph(
      vertices = vertexList,
      edges = adjList)

    Layouter.renderGraph(graph)
  }

  override def toString: String = {
    toString(v => v)
  }
}

object DAG {
  
  abstract class Adapter[D, M] {
    def id(obj: D): Int
    def metadata(obj: D): M
    def parents(obj: D): Seq[D]
  }
   
  def apply[D, M](leaf: D, adapter: Adapter[D, M]) = {
    val seen = new mutable.HashMap[Int, Vertex[M]]
    def build(obj: D): Vertex[M] = {
      val id = adapter.id(obj)
      val metadata = adapter.metadata(obj)
      val vertex = seen.getOrElseUpdate(id, new Vertex[M](id, metadata))
      val parents = adapter.parents(obj)
      parents.foreach({ parent =>
        val parentVertex = build(parent)
        parentVertex.addChild(vertex)
      })
      vertex
    }
    val oneRoot = build(leaf)
    new DAG(seen.filter(_._2.isRoot))
  }
}

private[spark] trait MetadataAttachable[T] {
  val metadata: T
}

private[spark] class Vertex[T](val id: Int, val metadata: T)
  extends MetadataAttachable[T] {

  val children =  mutable.HashMap[Int, Vertex[T]]()
  val parents = new mutable.HashMap[Int, Vertex[T]]()

  def isLeaf = children.isEmpty
  def isRoot = parents.isEmpty

  def addChild(child: Vertex[T]): Unit = {
    children.put(child.id, child)
    child.parents.put(this.id, this)
  }

  def addParent(parent: Vertex[T]): Unit = {
    parents.put(parent.id, parent)
    parent.children.put(this.id, this)
  }

  def visit(visitor: (Vertex[T]) => Unit, forward : Boolean = true): Unit = {
    visitor(this)
    val set = if (forward) children else parents
    set.foreach(_._2.visit(visitor, forward))
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Vertex[T]]

  override def equals(other: Any): Boolean = other match {
    case that: Vertex[T] =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = {
    s"Id: [$id]"
  }
}



