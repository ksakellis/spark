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

package org.apache.spark.io

import java.io.{InputStream, OutputStream}

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream}
import org.xerial.snappy.{Snappy, SnappyInputStream, SnappyOutputStream}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils
import org.apache.spark.Logging

/**
 * :: DeveloperApi ::
 * CompressionCodec allows the customization of choosing different compression implementations
 * to be used in block storage.
 *
 * Note: The wire protocol for a codec is not guaranteed compatible across versions of Spark.
 *       This is intended for use as an internal compression utility within a single
 *       Spark application.
 */
@DeveloperApi
trait CompressionCodec {

  def compressedOutputStream(s: OutputStream): OutputStream

  def compressedInputStream(s: InputStream): InputStream

  def isAvailable() : Boolean = true
}

private[spark] object CompressionCodec extends Logging {

  private val configKey = "spark.io.compression.codec"
  private val shortCompressionCodecNames = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName)

  def createCodec(conf: SparkConf): CompressionCodec = {
    conf.getOption(configKey)
        .map(createCodec(conf, _))
        .orElse(createCodecFromName(conf, DEFAULT_COMPRESSION_CODEC))
        .orElse({
          logWarning("Default codec " + DEFAULT_COMPRESSION_CODEC +
            " is unavailable. Faling back to " + FALLBACK_COMPRESSION_CODEC)
          createCodecFromName(conf, FALLBACK_COMPRESSION_CODEC)
        })
        .getOrElse(throw new IllegalArgumentException("The codec [" +
          FALLBACK_COMPRESSION_CODEC + "] is not available."))
  }

  def createCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    createCodecFromName(conf, codecName)
      .getOrElse(throw new IllegalArgumentException("The specified codec [" +
                  codecName + "] is not available."))
  }

  private def createCodecFromName(conf: SparkConf, codecName : String)
      : Option[CompressionCodec] = {
    val codecClass = shortCompressionCodecNames.getOrElse(codecName.toLowerCase, codecName)
    try {
      val ctor = Class.forName(codecClass, true, Utils.getContextOrSparkClassLoader)
        .getConstructor(classOf[SparkConf])
      Some(ctor.newInstance(conf).asInstanceOf[CompressionCodec])
        .filter(_.isAvailable())
    } catch {
      case e: ClassNotFoundException => None
    }
  }

  val DEFAULT_COMPRESSION_CODEC = "snappy"
  val FALLBACK_COMPRESSION_CODEC = "lzf"
  val ALL_COMPRESSION_CODECS = shortCompressionCodecNames.values.toSeq
}


/**
 * :: DeveloperApi ::
 * LZ4 implementation of [[org.apache.spark.io.CompressionCodec]].
 * Block size can be configured by `spark.io.compression.lz4.block.size`.
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 *       of Spark. This is intended for use as an internal compression utility within a single Spark
 *       application.
 */
@DeveloperApi
class LZ4CompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getInt("spark.io.compression.lz4.block.size", 32768)
    new LZ4BlockOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new LZ4BlockInputStream(s)
}


/**
 * :: DeveloperApi ::
 * LZF implementation of [[org.apache.spark.io.CompressionCodec]].
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 *       of Spark. This is intended for use as an internal compression utility within a single Spark
 *       application.
 */
@DeveloperApi
class LZFCompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new LZFOutputStream(s).setFinishBlockOnFlush(true)
  }

  override def compressedInputStream(s: InputStream): InputStream = new LZFInputStream(s)
}


/**
 * :: DeveloperApi ::
 * Snappy implementation of [[org.apache.spark.io.CompressionCodec]].
 * Block size can be configured by `spark.io.compression.snappy.block.size`.
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 *       of Spark. This is intended for use as an internal compression utility within a single Spark
 *       application.
 */
@DeveloperApi
class SnappyCompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getInt("spark.io.compression.snappy.block.size", 32768)
    new SnappyOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)

  override def isAvailable() = {
    try {
      Snappy.getNativeLibraryVersion
      true
    } catch {
      case e: Error => false
    }
  }
}
