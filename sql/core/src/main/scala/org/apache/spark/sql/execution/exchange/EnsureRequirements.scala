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

package org.apache.spark.sql.execution.exchange

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec,
  SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[ShuffleExchangeExec]] Operators where required.  Also ensure that
 * the input partition ordering requirements are met.
 */
case class EnsureRequirements(conf: SQLConf) extends Rule[SparkPlan] {
  private def defaultNumPreShufflePartitions: Int =
    if (conf.adaptiveExecutionEnabled) {
      conf.maxNumPostShufflePartitions
    } else {
      conf.numShufflePartitions
    }

  /**
   * Given a required distribution, returns a partitioning that satisfies that distribution.
   * @param requiredDistribution The distribution that is required by the operator
   * @param numPartitions Used when the distribution doesn't require a specific number of partitions
   */
  private def createPartitioning(
      requiredDistribution: Distribution,
      numPartitions: Int): Partitioning = {
    requiredDistribution match {
      case AllTuples => SinglePartition
      case ClusteredDistribution(clustering, desiredPartitions) =>
        HashPartitioning(clustering, desiredPartitions.getOrElse(numPartitions))
      case OrderedDistribution(ordering) => RangePartitioning(ordering, numPartitions)
      case dist => sys.error(s"Do not know how to satisfy distribution $dist")
    }
  }

  private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements:
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      case (child, distribution) =>
        ShuffleExchangeExec(createPartitioning(distribution, defaultNumPreShufflePartitions), child)
    }

    // If the operator has multiple children and specifies child output distributions (e.g. join),
    // then the children's output partitionings must be compatible:
    def requireCompatiblePartitioning(distribution: Distribution): Boolean = distribution match {
      case UnspecifiedDistribution => false
      case BroadcastDistribution(_) => false
      case _ => true
    }
    if (children.length > 1
        && requiredChildDistributions.exists(requireCompatiblePartitioning)
        && !Partitioning.allCompatible(children.map(_.outputPartitioning))) {

      // First check if the existing partitions of the children all match. This means they are
      // partitioned by the same partitioning into the same number of partitions. In that case,
      // don't try to make them match `defaultPartitions`, just use the existing partitioning.
      val maxChildrenNumPartitions = children.map(_.outputPartitioning.numPartitions).max
      val useExistingPartitioning = children.zip(requiredChildDistributions).forall {
        case (child, distribution) =>
          child.outputPartitioning.guarantees(
            createPartitioning(distribution, maxChildrenNumPartitions))
      }

      children = if (useExistingPartitioning) {
        // We do not need to shuffle any child's output.
        children
      } else {
        // We need to shuffle at least one child's output.
        // Now, we will determine the number of partitions that will be used by created
        // partitioning schemes.
        val numPartitions = {
          // Let's see if we need to shuffle all child's outputs when we use
          // maxChildrenNumPartitions.
          val shufflesAllChildren = children.zip(requiredChildDistributions).forall {
            case (child, distribution) =>
              !child.outputPartitioning.guarantees(
                createPartitioning(distribution, maxChildrenNumPartitions))
          }
          // If we need to shuffle all children, we use defaultNumPreShufflePartitions as the
          // number of partitions. Otherwise, we use maxChildrenNumPartitions.
          if (shufflesAllChildren) defaultNumPreShufflePartitions else maxChildrenNumPartitions
        }

        children.zip(requiredChildDistributions).map {
          case (child, distribution) =>
            val targetPartitioning = createPartitioning(distribution, numPartitions)
            if (child.outputPartitioning.guarantees(targetPartitioning)) {
              child
            } else {
              child match {
                // If child is an exchange, we replace it with
                // a new one having targetPartitioning.
                case ShuffleExchangeExec(_, c, _) => ShuffleExchangeExec(targetPartitioning, c)
                case _ => ShuffleExchangeExec(targetPartitioning, child)
              }
          }
        }
      }
    }

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        SortExec(requiredOrdering, global = false, child = child)
      }
    }

    operator.withNewChildren(children)
  }

  private def reorder(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      expectedOrderOfKeys: Seq[Expression],
      currentOrderOfKeys: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    val leftKeysBuffer = ArrayBuffer[Expression]()
    val rightKeysBuffer = ArrayBuffer[Expression]()

    expectedOrderOfKeys.foreach(expression => {
      val index = currentOrderOfKeys.indexWhere(e => e.semanticEquals(expression))
      leftKeysBuffer.append(leftKeys(index))
      rightKeysBuffer.append(rightKeys(index))
    })
    (leftKeysBuffer, rightKeysBuffer)
  }

  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {
    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      leftPartitioning match {
        case HashPartitioning(leftExpressions, _)
          if leftExpressions.length == leftKeys.length &&
            leftKeys.forall(x => leftExpressions.exists(_.semanticEquals(x))) =>
          reorder(leftKeys, rightKeys, leftExpressions, leftKeys)

        case _ => rightPartitioning match {
          case HashPartitioning(rightExpressions, _)
            if rightExpressions.length == rightKeys.length &&
              rightKeys.forall(x => rightExpressions.exists(_.semanticEquals(x))) =>
            reorder(leftKeys, rightKeys, rightExpressions, rightKeys)

          case _ => (leftKeys, rightKeys)
        }
      }
    } else {
      (leftKeys, rightKeys)
    }
  }

  /**
   * When the physical operators are created for JOIN, the ordering of join keys is based on order
   * in which the join keys appear in the user query. That might not match with the output
   * partitioning of the join node's children (thus leading to extra sort / shuffle being
   * introduced). This rule will change the ordering of the join keys to match with the
   * partitioning of the join nodes' children.
   */
  private def reorderJoinPredicates(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case BroadcastHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left,
        right) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        BroadcastHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right)

      case ShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right)

      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition, left, right)
    }
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator @ ShuffleExchangeExec(partitioning, child, _) =>
      child.children match {
        case ShuffleExchangeExec(childPartitioning, baseChild, _)::Nil =>
          if (childPartitioning.guarantees(partitioning)) child else operator
        case _ => operator
      }
    case operator: SparkPlan =>
      ensureDistributionAndOrdering(reorderJoinPredicates(operator))
  }
}
