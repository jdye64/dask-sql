use colored::Colorize;
use datafusion_common::{DataFusionError, Column};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, PlanVisitor};


// START - EXPLORING CODE

pub struct NodeSearchCriteria {

}

pub struct EndOfNeedVisitor;


impl PlanVisitor for EndOfNeedVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, _plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        println!("Pre-Visit: {:?}", _plan);
        Ok(true)
    }

    fn post_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        println!("Post-Visit: {:?}", plan);
        Ok(true)
    }
}

// END - EXPLORING CODE

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LogicalPlanType {
    AGGREGATE,
    ANY, // Any allows for any LogicalPlan variant to match on a search, think of it like a wildcard
    CROSS_JOIN,
    DISTINCT,
    JOIN,
}

pub struct OptimizablePlan {
    original_plan: LogicalPlan,
    search_criteria: Vec<LogicalPlanType>,
    visitor: Box<dyn PlanVisitor<Error = DataFusionError>>,
    current_idx: usize,
    match_found: bool,
    left_plan_nodes: Vec<LogicalPlan>,
    match_plan_nodes: Vec<LogicalPlan>,
    right_plan_nodes: Vec<LogicalPlan>,
}

impl OptimizablePlan {
    pub fn new(plan: LogicalPlan, search_criteria: Vec<LogicalPlanType>) -> Self {
        Self {
            original_plan: plan,
            search_criteria: search_criteria,
            visitor: Box::new(EndOfNeedVisitor),
            current_idx: 0,
            match_found: false,
            left_plan_nodes: Vec::new(),
            match_plan_nodes: Vec::new(),
            right_plan_nodes: Vec::new(),
        }
    }

    /// Given a `PlanSearchExpr` attempt to locate the portion of the plan
    /// described in the Expr/Criteria in the `LogicalPlan`. If a match
    /// if found a response is generated containing the dissected bits of the
    /// search. The left side, matched portion, and right side of the plan
    /// so that further operations can be achieved much more easily.
    pub fn find(&mut self) -> (Vec<LogicalPlan>, Vec<LogicalPlan>, Vec<LogicalPlan>) {
        let vis: &mut dyn PlanVisitor<Error = DataFusionError> = &mut *self.visitor;
        let _find_result = self.original_plan.clone().accept(vis);
        (
            self.left_plan_nodes.clone(),
            self.match_plan_nodes.clone(),
            self.right_plan_nodes.clone(),
        )
    }

    pub fn replace_match_with(&mut self, replace: Vec<LogicalPlan>) {
        self.match_plan_nodes = replace;
    }

    /// Find a complete math and replace it with the provided `LogicalPlan`
    pub fn find_replace(
        &mut self,
        replace: LogicalPlan,
    ) -> (Vec<LogicalPlan>, Vec<LogicalPlan>, Vec<LogicalPlan>) {
        self.find()
    }

    pub fn print_plan(&self) {
        println!(
            "Left_Plan_Nodes: {}, Match_Plan_Nodes: {}, Right_Plan_Nodes: {}",
            self.left_plan_nodes.len().to_string().red(),
            self.match_plan_nodes.len().to_string().blue(),
            self.right_plan_nodes.len().to_string().green()
        );
    }

    /// Takes the current `left_plan_nodes`, `match_plan_nodes` and `right_plan_nodes` and rebuilds
    /// a complete `LogicalPlan`
    pub fn rebuild(&mut self) -> LogicalPlan {
        let mut builder: LogicalPlanBuilder = LogicalPlanBuilder::empty(false);

        // Plans are build in reverse. Start with the `right_plan_nodes` and also
        // reverse them to achieve the correct order
        self.right_plan_nodes.reverse();
        for p in &self.right_plan_nodes {
            builder = match p {
                LogicalPlan::Aggregate(agg) => builder
                    .aggregate(agg.group_expr.clone(), agg.aggr_expr.clone())
                    .expect("invalid aggregate node"),
                LogicalPlan::Distinct(_) => builder.distinct().expect("invalid distinct node"),
                LogicalPlan::TableScan(scan) => {
                    LogicalPlanBuilder::from(LogicalPlan::TableScan(scan.clone()))
                }
                _ => panic!("Error, encountered: {:?}", p),
            }
        }

        // Continue with `match_plan_nodes`
        self.match_plan_nodes.reverse();
        for p in &self.match_plan_nodes {
            builder = match p {
                LogicalPlan::Aggregate(agg) => builder
                    .aggregate(agg.group_expr.clone(), agg.aggr_expr.clone())
                    .expect("invalid aggregate node"),
                LogicalPlan::Distinct(_) => builder.distinct().expect("invalid distinct node"),
                LogicalPlan::Projection(projection) => builder
                    .project(projection.expr.clone())
                    .expect("invalid projection node"),
                LogicalPlan::TableScan(scan) => {
                    LogicalPlanBuilder::from(LogicalPlan::TableScan(scan.clone()))
                }
                _ => panic!("Error, encountered: {:?}", p),
            }
        }

        // Finish with `left_plan_nodes`
        self.left_plan_nodes.reverse();
        for p in &self.left_plan_nodes {
            builder = match p {
                LogicalPlan::Aggregate(agg) => builder
                    .aggregate(agg.group_expr.clone(), agg.aggr_expr.clone())
                    .expect("invalid aggregate node"),
                LogicalPlan::Distinct(_) => builder.distinct().expect("invalid distinct node"),
                LogicalPlan::Projection(projection) => builder
                    .project(projection.expr.clone())
                    .expect("invalid projection node"),
                LogicalPlan::TableScan(scan) => {
                    LogicalPlanBuilder::from(LogicalPlan::TableScan(scan.clone()))
                }
                LogicalPlan::Join(join) => {
                    let left_cols: Vec<Column> = Vec::new();
                    let right_cols: Vec<Column> = Vec::new();
                    builder.join(
                        &join.right,
                        datafusion_expr::JoinType::Inner,
                        (left_cols, right_cols),
                        None
                    ).expect("invalid join node")
                }
                _ => panic!("Error, encountered: {:?}", p),
            }
        }

        builder.build().unwrap()
    }

    /// Utility method for checking if the `current_criteria` node matches
    /// the `LogicalPlan` variant. If so the `current_idx` is incremented.
    fn does_match(
        &self,
        left_plan_type: &LogicalPlanType,
        right_plan_type: &LogicalPlanType,
    ) -> usize {
        if *left_plan_type == *right_plan_type {
            self.current_idx + 1
        } else {
            0
        }
    }
}

impl PlanVisitor for OptimizablePlan {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        // Ok lets see if we can match the search_criteria against these nodes
        if !self.match_found {
            // Push the current plan node into the left_plan_nodes, if a match is later determined it will be popped and moved
            self.left_plan_nodes.push(plan.clone());

            let cc = self.search_criteria.get(self.current_idx).unwrap();
            self.current_idx = match plan {
                LogicalPlan::Aggregate(_) => self.does_match(cc, &LogicalPlanType::AGGREGATE),
                LogicalPlan::CrossJoin(_) => self.does_match(cc, &LogicalPlanType::CROSS_JOIN),
                LogicalPlan::Distinct(_) => self.does_match(cc, &LogicalPlanType::DISTINCT),
                LogicalPlan::Join(_) => self.does_match(cc, &LogicalPlanType::JOIN),
                _ => 0,
            };

            // We have a complete match if this condition is met
            if self.current_idx >= self.search_criteria.len() {
                self.match_found = true;

                // All criteria has been met. This constitutes a successful location of all search criteria
                // move the Nth elements from the `left_plan_nodes` into the `match_plan_nodes`
                for _idx in &self.search_criteria {
                    self.match_plan_nodes
                        .push(self.left_plan_nodes.pop().unwrap());
                }
            }
        } else {
            // We already have a complete search criteria match, that means anything encountered and and beyond should be included
            // in the `right_plan_side` of the search results
            self.right_plan_nodes.push(plan.clone());
        }

        Ok(true)
    }

    fn post_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        datasource::{empty::EmptyTable, provider_as_source},
        logical_expr::UNNAMED_TABLE,
    };
    use datafusion_common::DataFusionError;
    use datafusion_expr::{col, LogicalPlan, LogicalPlanBuilder, logical_plan::builder::LogicalTableSource, JoinType, sum};

    use super::LogicalPlanType;
    use crate::sql::optimizer::utils;

    /// Scan an empty data source, mainly used in tests
    fn scan_empty(
        name: Option<&str>,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let table_schema = Arc::new(table_schema.clone());
        let provider = Arc::new(EmptyTable::new(table_schema));
        LogicalPlanBuilder::scan(
            name.unwrap_or(UNNAMED_TABLE),
            provider_as_source(provider),
            projection,
        )
    }

    /// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
    /// This is mostly used for testing and documentation.
    pub fn table_scan(
        name: Option<&str>,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
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

    #[test]
    fn test_optimizable_plan_visitor() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        // Dummy LogicalPlan with 2 subsequent DISTINCT nodes
        let logical_plan = scan_empty(Some("test"), &schema, None)?
            .distinct()?
            .distinct()?
            .project(vec![col("id")])?
            .build();

        // Creates an `OptimizablePlan` instance with a `search_criteria` that 
        // dictates the nodes that should be search for
        let mut opt_plan = utils::OptimizablePlan::new(
            logical_plan?,
            vec![LogicalPlanType::DISTINCT, LogicalPlanType::DISTINCT],
        );

        // Attempts to locate the interesting area of the Optimizer
        opt_plan.find();

        // Replaces the previous match, which in this example is a DISTINCT followed by another DISTINCT
        // as described in the `search_criteria` when creating the `OptimizablePlan` with a single
        // `LogicalPlan::DISTINCT` created with the `LogicalPlanBuilder`, could be multiple nodes ...
        opt_plan.replace_match_with(vec![LogicalPlanBuilder::empty(false).distinct()?.build()?]);

        // Rebuilds a single `LogicalPlan` instance from all the moving parts
        let optimized_plan: LogicalPlan = opt_plan.rebuild();

        println!("Optimized Plan: \n{:?}", optimized_plan);

        Ok(())
    }



    /// A query like
    /// ```text
    /// SELECT
    ///     SUM(df.a), df2.b
    /// FROM df
    /// INNER JOIN df2
    ///     ON df.c = df2.c
    /// GROUP BY df2.b
    /// ```
    ///
    /// Would typically produce a LogicalPlan like ...
    /// ```text
    /// Projection: SUM(df.a), df2.b\
    ///   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
    ///     Inner Join: df.c = df2.c\
    ///       TableScan: df projection=[a, c], full_filters=[df.c IS NOT NULL]\
    ///       TableScan: df2 projection=[b, c], full_filters=[df2.c IS NOT NULL]\
    /// ```
    ///
    /// Where df.c and df2.c would be unnecessarily carried into the aggregate step even though it can
    /// be dropped.
    ///
    /// To solve this problem, we insert a projection after the join step. In our example, the
    /// optimized LogicalPlan is
    /// ```text
    /// Projection: SUM(df.a), df2.b\
    ///   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
    ///     Projection: df.a, df2.b\
    ///       Inner Join: df.c = df2.c\
    ///         TableScan: df projection=[a, c], full_filters=[df.c IS NOT NULL]\
    ///         TableScan: df2 projection=[b, c], full_filters=[df2.c IS NOT NULL]\
    #[test]
    fn test_remove_extra_column_baggage() -> Result<(), DataFusionError> {

        // Projection: SUM(df.a), df2.b
        //   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]
        //     Inner Join: df.c = df2.c
        //       TableScan: df
        //       TableScan: df2
        let plan = LogicalPlanBuilder::from(test_table_scan("df", "a"))
            .join(
                &LogicalPlanBuilder::from(test_table_scan("df2", "b")).build()?,
                JoinType::Inner,
                (vec!["c"], vec!["c"]),
                None,
            )?
            .aggregate(vec![col("df2.b")], vec![sum(col("df.a"))])?
            .project(vec![sum(col("df.a")), col("df2.b")])?
            .build()?;


        // Creates an `OptimizablePlan` instance with a `search_criteria` that 
        // dictates the nodes that should be search for
        let mut opt_plan = utils::OptimizablePlan::new(
            plan,
            vec![LogicalPlanType::DISTINCT, LogicalPlanType::DISTINCT],
        );

        // Attempts to locate the interesting area of the Optimizer
        opt_plan.find();

        opt_plan.replace_match_with(vec![LogicalPlanBuilder::empty(false).distinct()?.build()?]);

        // Rebuilds a single `LogicalPlan` instance from all the moving parts
        let optimized_plan: LogicalPlan = opt_plan.rebuild();

        println!("Optimized Plan: \n{:?}", optimized_plan);


        Ok(())
    }

}
