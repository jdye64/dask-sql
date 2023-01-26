use std::collections::LinkedList;

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{LogicalPlan, PlanVisitor};

#[derive(Debug, Clone)]
pub struct PlanSearchExpr {
    plan: Option<LogicalPlan>,
}

// impl PlanSearchExpr {
//     pub fn empty_follow_by(plan: LogicalPlan) -> Self {

//     }
// }

#[derive(Debug, Clone)]
pub struct PlanSearchExprBuilder {
    search_expr: PlanSearchExpr,
}

impl PlanSearchExprBuilder {
    pub fn new() -> Self {
        Self {
            search_expr: PlanSearchExpr { plan: None },
        }
    }

    /// Create a builder from an existing plan
    pub fn from(search_expr: PlanSearchExpr) -> Self {
        Self { search_expr }
    }

    /// Build the plan
    pub fn build(&self) -> Result<PlanSearchExpr> {
        Ok(self.search_expr.clone())
    }

    // pub fn followed_by(plan: LogicalPlan) -> Result<Self>{
    //     Ok(Self::from(PlanSearchExpr::empty_follow_by(plan)))
    // }
}

#[derive(Debug)]
pub struct OptimizablePlan {
    original_plan: LogicalPlan,
    flex_plan: LinkedList<LogicalPlan>,
}

impl OptimizablePlan {
    pub fn new(plan: LogicalPlan) -> Self {
        println!(
            "Create OptimizablePlan with LogicalPlan instance: {:?}",
            plan
        );
        Self {
            original_plan: plan,
            flex_plan: LinkedList::new(),
        }
    }

    /// Traverses in the input LogicalPlan (DAG) and build a doubly LinkedList from it
    /// The LinkedList structure is used because it makes it much more simple for
    /// optimizer rules to be written that involve "slicing and fusing" the DAG/LogicalPlan
    /// into a new optimized format.
    pub fn build(&mut self) {
        let build_result = self.original_plan.clone().accept(self);
        println!("Build Result: {:?}", build_result);
        println!("Flex Plan: {:?}", self.flex_plan);
    }

    /// Given a `PlanSearchExpr` attempt to locate the portion of the plan
    /// described in the Expr/Criteria in the `LogicalPlan`. If a match
    /// if found a response is generated containing the dissected bits of the
    /// search. The left side, matched portion, and right side of the plan
    /// so that further operations can be achieved much more easily.
    pub fn find(
        &self,
        search_expr: PlanSearchExpr,
    ) -> (Option<LogicalPlan>, LogicalPlan, Option<LogicalPlan>) {
        println!(
            "Ok - We have entered the core of the logic. Lets search for what the user wants ..."
        );
    }
}

impl PlanVisitor for OptimizablePlan {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        // If the plan contains an unsupported Node type we flag the plan as un-optimizable here
        println!("pre_visit: {:?}", plan);
        match plan {
            LogicalPlan::Explain(..) => Ok(false),
            _ => Ok(true),
        }
    }

    fn post_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        println!("post_visit: {:?}", plan);

        // Insert the LogicalPlan into the LinkedList
        self.flex_plan.push_back(plan.clone());

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
    use datafusion_expr::{col, LogicalPlanBuilder};

    use super::{PlanSearchExpr, PlanSearchExprBuilder};
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

    // /// Creates a `PlanSearchExpr` that searches for a `LogicalPlan::DISTINCT` node that
    // /// immediately follows another `LogicalPlan::DISTINCT` node.
    // pub fn search_plan_distinct_distinct() -> Result<PlanSearchExpr, DataFusionError> {
    //     let search = PlanSearchExpr::new();
    //     Ok(search)
    // }

    #[test]
    fn test_optimizable_plan_visitor() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("test"), &schema, None)
            .expect("invalid LogicalPlanBuild")
            .project(vec![col("id")])
            .expect("invalid LogicalPlanBuilder")
            .build();

        let mut opt_plan = utils::OptimizablePlan::new(logical_plan.unwrap());
        opt_plan.build();

        // Attempts to locate the inresteing area of the Optimizer
        let builder = PlanSearchExprBuilder::new().build().unwrap();
        opt_plan.find(builder);
    }
}
