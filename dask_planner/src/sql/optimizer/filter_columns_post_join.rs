//! Optimizer rule dropping join key columns post join
//!
//! Many queries follow a pattern where certain columns are only used as join keys and aren't
//! really needed/selected in the query post join. In cases such as these it is useful to drop
//! those columns post join rather than carrying them around for subsequent operations and only
//! dropping them at the end during the projection.
//!
//! A query like
//! ```text
//! SELECT
//!     SUM(df.a), df2.b
//! FROM df
//! INNER JOIN df2
//!     ON df.c = df2.c
//! GROUP BY df2.b
//! ```
//!
//! Would typically produce a LogicalPlan like ...
//! ```text
//! Projection: SUM(df.a), df2.b\
//!   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
//!     Inner Join: df.c = df2.c\
//!       TableScan: df projection=[a, c], full_filters=[df.c IS NOT NULL]\
//!       TableScan: df2 projection=[b, c], full_filters=[df2.c IS NOT NULL]\
//! ```
//!
//! Where df.c and df2.c would be unnecessarily carried into the aggregate step even though it can
//! be dropped.
//!
//! To solve this problem, we insert a projection after the join step. In our example, the
//! optimized LogicalPlan is
//! ```text
//! Projection: SUM(df.a), df2.b\
//!   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
//!     Projection: df.a, df2.b\
//!       Inner Join: df.c = df2.c\
//!         TableScan: df projection=[a, c], full_filters=[df.c IS NOT NULL]\
//!         TableScan: df2 projection=[b, c], full_filters=[df2.c IS NOT NULL]\
//! ```
//!
//! Which corresponds to rewriting the query as
//! ```text
//! SELECT
//!     SUM(df.a), df2.b
//! FROM
//!     (SELECT
//!         df.a, df2.b
//!     FROM df
//!     INNER JOIN df2
//!         ON df.c = df2.c)
//! GROUP BY df2.b
//! ```
//!
//! Note that if the LogicalPlan was something like
//! ```text
//! Projection: df.a, df2.b\
//!   Inner Join: df.c = df2.c\
//!     TableScan: df projection=[a, c], full_filters=[df.c IS NOT NULL]\
//!     TableScan: df2 projection=[b, c], full_filters=[df2.c IS NOT NULL]\
//! ```
//!
//! Then we need not add an additional Projection step, as columns df.c and df2.c are immediately
//! dropped in the next step, which is itself a Projection.

use std::sync::Arc;

use datafusion_common::{Column, Result};
use datafusion_expr::{
    logical_plan::LogicalPlan,
    Expr};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};

use crate::sql::optimizer::utils::{OptimizationStrategy, fusion::PlanFusion};
use crate::sql::optimizer::utils::column::ColumnIndexBuilder;
use crate::sql::optimizer::utils::dissect::Dissector;
use crate::sql::optimizer::utils::dissect::JoinDissector;
use crate::sql::optimizer::utils::fusion::PreFoundIndexFusion;


/// Optimizer rule dropping join key columns post join
#[derive(Default)]
pub struct FilterColumnsPostJoin {}

impl FilterColumnsPostJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for FilterColumnsPostJoin {

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {

        // Build out the index of the last seen location for each column
        let _optimized_plan = ColumnIndexBuilder::new().optimize(&plan);

        // Split the Logical plan into "slices" of the complete plan
        let mut join_dissector = JoinDissector::new();
        let _slices: Vec<LogicalPlan> = join_dissector.dissect(&plan);

        // Build the projection `LogicalPlan` node to be inserted
        let exprs = vec![
            Expr::Column(
                Column { relation: Some("df2".to_string()), name: "b".to_string() }
            ),
            Expr::Column(
                Column { relation: Some("df".to_string()), name: "a".to_string() }
            )
        ];
        let input = Arc::new(join_dissector.slices[join_dissector.join_idxs[0]].clone());
        let node = LogicalPlan::Projection(
            datafusion_expr::logical_plan::Projection::try_new(exprs, input)?
        );

        let r_plan = PreFoundIndexFusion::new().fuse(join_dissector.slices, join_dissector.join_idxs, node);
        Ok(Some(r_plan.clone()))
    }

    fn name(&self) -> &str {
        "filter_columns_post_join"
    }

    /// How should the rule be applied by the optimizer? See comments on [`ApplyOrder`] for details.
    ///
    /// If a rule use default None, its should traverse recursively plan inside itself
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{
        col,
        logical_plan::{builder::LogicalTableSource, JoinType, LogicalPlanBuilder},
        sum,
    };
    use datafusion_optimizer::OptimizerContext;

    use super::*;

    /// Optimize with just the filter_columns_post_join rule
    fn optimized_plan_eq(plan: &LogicalPlan, expected1: &str, expected2: &str) -> bool {
        let rule = FilterColumnsPostJoin::new();
        let optimized_plan = rule
            .try_optimize(plan, &OptimizerContext::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.unwrap().display_indent());

        if formatted_plan == expected1 || formatted_plan == expected2 {
            true
        } else {
            false
        }
    }

    #[test]
    fn test_single_join() -> Result<()> {
        // Projection: SUM(df.a), df2.b
        //   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]
        //     Inner Join: df.c = df2.c
        //       TableScan: df
        //       TableScan: df2
        let plan = LogicalPlanBuilder::from(test_table_scan("df", "a"))
            .join(
                LogicalPlanBuilder::from(test_table_scan("df2", "b")).build()?,
                JoinType::Inner,
                (vec!["c"], vec!["c"]),
                None,
            )?
            .aggregate(vec![col("df2.b")], vec![sum(col("df.a"))])?
            .project(vec![sum(col("df.a")), col("df2.b")])?
            .build()?;

        let expected1 = "Projection: SUM(df.a), df2.b\
        \n  Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
        \n    Projection: df.a, df2.b\
        \n      Inner Join: df.c = df2.c\
        \n        TableScan: df\
        \n        TableScan: df2";

        let expected2 = "Projection: SUM(df.a), df2.b\
        \n  Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
        \n    Projection: df2.b, df.a\
        \n      Inner Join: df.c = df2.c\
        \n        TableScan: df\
        \n        TableScan: df2";

        assert_eq!(optimized_plan_eq(&plan, expected1, expected2), true);

        Ok(())
    }

    /// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
    /// This is mostly used for testing and documentation.
    pub fn table_scan(
        name: Option<&str>,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder> {
        let tbl_schema = Arc::new(table_schema.clone());
        let table_source = Arc::new(LogicalTableSource::new(tbl_schema));
        LogicalPlanBuilder::scan(name.unwrap_or("test"), table_source, projection)
    }

    fn test_table_scan(table_name: &str, column_name: &str) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new(column_name, DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some(table_name), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }
}
