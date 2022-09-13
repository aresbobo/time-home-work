package com.test

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Literal, NullIf, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      MyPushDown(session)
    }
  }
}

case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
  logWarning(msg = "MyPushDown Start")

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case e: RuntimeReplaceable => e.child
    case CountIf(predicate) => Count(new NullIf(predicate, Literal.FalseLiteral))
    case BoolOr(arg) => Max(arg)
    case BoolAnd(arg) => Min(arg)
  }
}

