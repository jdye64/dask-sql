use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::{
    common_subexpr_eliminate::CommonSubexprEliminate,
    decorrelate_where_exists::DecorrelateWhereExists,
    decorrelate_where_in::DecorrelateWhereIn,
    eliminate_cross_join::EliminateCrossJoin,
    // TODO: need to handle EmptyRelation for GPU cases
    // eliminate_filter::EliminateFilter,
    eliminate_limit::EliminateLimit,
    eliminate_outer_join::EliminateOuterJoin,
    filter_null_join_keys::FilterNullJoinKeys,
    inline_table_scan::InlineTableScan,
    limit_push_down::LimitPushDown,
    optimizer::{Optimizer, OptimizerRule},
    projection_push_down::ProjectionPushDown,
    push_down_filter::PushDownFilter,
    rewrite_disjunctive_predicate::RewriteDisjunctivePredicate,
    scalar_subquery_to_join::ScalarSubqueryToJoin,
    simplify_expressions::SimplifyExpressions,
    subquery_filter_to_join::SubqueryFilterToJoin,
    type_coercion::TypeCoercion,
    unwrap_cast_in_comparison::UnwrapCastInComparison,
    OptimizerConfig,
};
use log::trace;

mod eliminate_agg_distinct;
use eliminate_agg_distinct::EliminateAggDistinct;

mod utils;

/// Houses the optimization logic for Dask-SQL. This optimization controls the optimizations
/// and their ordering in regards to their impact on the underlying `LogicalPlan` instance
pub struct DaskSqlOptimizer {
    skip_failing_rules: bool,
    optimizer: Optimizer,
}

impl DaskSqlOptimizer {
    /// Creates a new instance of the DaskSqlOptimizer with all the DataFusion desired
    /// optimizers as well as any custom `OptimizerRule` trait impls that might be desired.
    pub fn new(skip_failing_rules: bool) -> Self {
        let rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
            Arc::new(InlineTableScan::new()),
            Arc::new(TypeCoercion::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(DecorrelateWhereExists::new()),
            Arc::new(DecorrelateWhereIn::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(SubqueryFilterToJoin::new()),
            // simplify expressions does not simplify expressions in subqueries, so we
            // run it again after running the optimizations that potentially converted
            // subqueries to joins
            Arc::new(SimplifyExpressions::new()),
            // TODO: need to handle EmptyRelation for GPU cases
            // Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(RewriteDisjunctivePredicate::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(LimitPushDown::new()),
            // Dask-SQL specific optimizations
            Arc::new(EliminateAggDistinct::new()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(ProjectionPushDown::new()),
        ];

        Self {
            skip_failing_rules,
            optimizer: Optimizer::with_rules(rules),
        }
    }

    /// Iteratoes through the configured `OptimizerRule`(s) to transform the input `LogicalPlan`
    /// to its final optimized form
    pub(crate) fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        let mut config =
            OptimizerConfig::default().with_skip_failing_rules(self.skip_failing_rules);
        self.optimizer.optimize(&plan, &mut config, Self::observe)
    }

    fn observe(optimized_plan: &LogicalPlan, optimization: &dyn OptimizerRule) {
        trace!(
            "== AFTER APPLYING RULE {} ==\n{}\n",
            optimization.name(),
            optimized_plan.display_indent()
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{any::Any, collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{DataFusionError, Result, ScalarValue};
    use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource};
    use datafusion_sql::{
        planner::{ContextProvider, SqlToRel},
        sqlparser::{ast::Statement, parser::Parser},
        TableReference,
    };

    use crate::{dialect::DaskDialect, sql::optimizer::DaskSqlOptimizer};

    #[test]
    fn subquery_filter_with_cast() -> Result<()> {
        // regression test for https://github.com/apache/arrow-datafusion/issues/3760
        let sql = "SELECT col_int32 FROM test \
    WHERE col_int32 > (\
      SELECT AVG(col_int32) FROM test \
      WHERE col_utf8 BETWEEN '2002-05-08' \
        AND (cast('2002-05-08' as date) + interval '5 days')\
    )";
        let plan = test_sql(sql)?;
        let expected = r#"Projection: test.col_int32
  Filter: CAST(test.col_int32 AS Float64) > __sq_1.__value
    CrossJoin:
      TableScan: test projection=[col_int32]
      SubqueryAlias: __sq_1
        Projection: AVG(test.col_int32) AS __value
          Aggregate: groupBy=[[]], aggr=[[AVG(test.col_int32)]]
            Filter: test.col_utf8 >= Utf8("2002-05-08") AND test.col_utf8 <= Utf8("2002-05-13")
              TableScan: test projection=[col_int32, col_utf8]"#;
        assert_eq!(expected, format!("{:?}", plan));
        Ok(())
    }

    fn test_sql(sql: &str) -> Result<LogicalPlan> {
        // parse the SQL
        let dialect = DaskDialect {};
        let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
        let statement = &ast[0];

        // create a logical query plan
        let schema_provider = MySchemaProvider {};
        let sql_to_rel = SqlToRel::new(&schema_provider);
        let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

        // optimize the logical plan
        let optimizer = DaskSqlOptimizer::new(false);
        optimizer.optimize(plan)
    }

    struct MySchemaProvider {}

    impl ContextProvider for MySchemaProvider {
        fn get_table_provider(
            &self,
            name: TableReference,
        ) -> datafusion_common::Result<Arc<dyn TableSource>> {
            let table_name = name.table();
            if table_name.starts_with("test") {
                let schema = Schema::new_with_metadata(
                    vec![
                        Field::new("col_int32", DataType::Int32, true),
                        Field::new("col_uint32", DataType::UInt32, true),
                        Field::new("col_utf8", DataType::Utf8, true),
                        Field::new("col_date32", DataType::Date32, true),
                        Field::new("col_date64", DataType::Date64, true),
                    ],
                    HashMap::new(),
                );

                Ok(Arc::new(MyTableSource {
                    schema: Arc::new(schema),
                }))
            } else {
                Err(DataFusionError::Plan("table does not exist".to_string()))
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
            None
        }

        fn get_config_option(&self, _option: &str) -> Option<ScalarValue> {
            None
        }
    }

    struct MyTableSource {
        schema: SchemaRef,
    }

    impl TableSource for MyTableSource {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }
}
