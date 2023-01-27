use colored::Colorize;
use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, PlanVisitor};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LogicalPlanType {
    AGGREGATE,
    ANY, // Any allows for any LogicalPlan variant to match on a search, think of it like a wildcard
    CROSS_JOIN,
    DISTINCT,
    JOIN,
}

#[derive(Debug)]
pub struct OptimizablePlan {
    original_plan: LogicalPlan,
    search_criteria: Vec<LogicalPlanType>,
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
        let _find_result = self.original_plan.clone().accept(self);
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
    use datafusion_expr::{col, LogicalPlan, LogicalPlanBuilder};

    use super::LogicalPlanType;
    use crate::sql::optimizer::utils;

    /// Scan an empty data source, mainly used in tests
    pub fn scan_empty(
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

    #[test]
    fn test_optimizable_plan_visitor() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("test"), &schema, None)
            .expect("invalid LogicalPlanBuild")
            .distinct()
            .expect("invalid builder")
            .distinct()
            .expect("invalid")
            .project(vec![col("id")])
            .expect("invalid LogicalPlanBuilder")
            .build();

        let mut opt_plan = utils::OptimizablePlan::new(
            logical_plan.unwrap(),
            vec![LogicalPlanType::DISTINCT, LogicalPlanType::DISTINCT],
        );
        // Attempts to locate the interesting area of the Optimizer
        let _find_results = opt_plan.find();
        let replace = LogicalPlanBuilder::empty(false).distinct().unwrap();
        opt_plan.replace_match_with(vec![replace.build().unwrap()]);
        let optimized_plan: LogicalPlan = opt_plan.rebuild();

        println!("Optimized Plan: \n{:?}", optimized_plan);
    }
}
