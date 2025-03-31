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

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging

class StageListener extends SparkListener with Logging {


  val performanceEstimator = new PerformanceEstimator


  def getRuntimeEstimate(stageId: Int): Long = {
    performanceEstimator.getRuntimeEstimate(stageId)
  }

  def getStageEndTimes: java.util.List[(Integer, java.lang.Long)] = {
    performanceEstimator.getStageEndTimes
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logInfo("####### Stage completed: " + stageCompleted.stageInfo.stageId)
    performanceEstimator.registerStageCompleted(stageCompleted)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    logInfo("####### Stage submitted: " + stageSubmitted.stageInfo.stageId)
    performanceEstimator.estimateStage(stageSubmitted)
  }
}

class StageProfile(jobSecId: String, stageId: Int, estimate: Long) {
  val jobSectionId: String = jobSecId
  val stageIds: ArrayBuffer[Int] = ArrayBuffer(stageId)
  var estimatedTime: Long = estimate

}

class PerformanceEstimator extends Logging {
  val JOB_CLASS_PROPERTY = "job.class"
  val DEFAULT_RUNTIME = 1000L

  val lock = new Object
  var stageEndtimes = ListBuffer[(Int, Long)]()
  val stageIdToProfile = TrieMap[Int, StageProfile]()
  val stageSecIdToProfile = TrieMap[String, StageProfile](
    "jobs.implementations.ShortOperation-compute" ->
      new StageProfile("jobs.implementations.ShortOperation-compute", -1, 20000L),
    "jobs.implementations.LongOperation-compute" ->
      new StageProfile("jobs.implementations.LongOperation-compute", -1, 235740L),
    "jobs.implementations.SuperShortOperation-compute" ->
      new StageProfile("jobs.implementations.SuperShortOperation-compute", -1, 4000L)

  )

  def registerStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    lock.synchronized {
      stageEndtimes += ((stageId, System.currentTimeMillis()))
    }

  }


  def estimateStage(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    // First get the section of the job
    val jobClass = stageSubmitted.properties.getProperty(JOB_CLASS_PROPERTY, "DEFAULT")
    val stageSection = getStageSection(stageSubmitted);
    val stageSecId = jobClass + "-" + stageSection

    val stageId = stageSubmitted.stageInfo.stageId
    logInfo("####### Stage section id: " + stageSecId + " for stageid: " + stageId)
    val profile = stageSecIdToProfile.getOrElse(stageSecId, {
      logInfo("####### no profile found for: " + stageSecId)
      val newProfile = new StageProfile(stageSecId, stageId, 1000L)
      stageSecIdToProfile.put(stageSecId, newProfile)
      newProfile
    })

    stageIdToProfile.put(stageId, profile)
  }

  def getStageEndTimes: java.util.List[(Integer, java.lang.Long)] = lock.synchronized {
    val copy = stageEndtimes.map { case (id, time) => Tuple2(id: Integer, time: java.lang.Long) }
      .toList
      .asJava

    stageEndtimes.clear()
    copy
  }


  def getRuntimeEstimate(stageId: Int): Long = stageIdToProfile.get(stageId) match {
      case Some(e) => e.estimatedTime
      case None => logInfo("####### no profile found for: " + stageId); DEFAULT_RUNTIME
    }

  val READ_SECTION = "ParallelCollectionRDD"
  val WRITE_SECTION = "ShuffledRowRDD"
  val COMPUTE_SECTION = "FileScanRDD"

  private def getStageSection(submitted: SparkListenerStageSubmitted): String = {
    var section = "UNKNOWN"
    for(rdd <- submitted.stageInfo.rddInfos) {
      rdd.name match {
        case n if n.contains(READ_SECTION) => section = "read"
        case n if n.contains(WRITE_SECTION) => section = "write"
        case n if n.contains(COMPUTE_SECTION) => section = "compute"
        case _ =>
      }
    }

    section
  }
}



