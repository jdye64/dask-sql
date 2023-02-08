use colored::Colorize;
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, PlanVisitor};

use self::{
    column::ColumnIndexBuilder,
    dissect::{Dissector, JoinDissector},
    fusion::{PlanFusion, PreFoundIndexFusion}
};

pub mod column;
pub mod dissect;
pub mod fusion;

/// Represents a strategy that can be used to optimize a `LogicalPlan`. While all implementations
/// of a strategy will be different they share these same methods for interaction with the higher
/// level `OptimizablePlanCtx`
pub trait OptimizationStrategy : PlanVisitor {

    /// Invokes the main portion of the optimization
    fn optimize(&mut self, input_plan: &LogicalPlan) -> LogicalPlan;

    /// During the course of optimization there exists the possibility that the input `LogicalPlan`
    /// has been "broken apart" and modified. This method is invoked to rebuild those disparate pieces
    /// back into a single, optimized, `LogicalPlan`
    fn rebuild(&mut self) -> LogicalPlan;
}

pub struct OptimizablePlanCtx {
    input_plan: LogicalPlan,
    index_builder: ColumnIndexBuilder,
    dissector: JoinDissector,
    fusion: PreFoundIndexFusion,

    // Holds the resulting rebuilt `LogicalPlan` after all modifications have occured
    final_plan: Option<LogicalPlan>,
}

impl OptimizablePlanCtx {

    pub fn new(input_plan: LogicalPlan) -> Self {
        Self {
            input_plan,
            index_builder: ColumnIndexBuilder::new(),
            dissector: JoinDissector::new(),
            fusion: PreFoundIndexFusion::new(),
            final_plan: None,
        }
    }

    pub fn build_index(&mut self) -> &mut Self {
        // Build out the index of the last seen location for each column
        self.index_builder.optimize(&self.input_plan);
        self
    }

    pub fn slice_at(&mut self, node_type: LogicalPlanType) -> &mut Self {
        let _slices: Vec<LogicalPlan> = self.dissector.dissect(&self.input_plan);
        self
    }

    pub fn insert_before(&mut self, node: LogicalPlan) -> &mut Self {
        self.final_plan = Some(self.fusion.fuse(self.dissector.slices.clone(), self.dissector.join_idxs.clone(), node));
        self
    }

    pub fn build(&mut self) -> Option<LogicalPlan> {
        self.final_plan.clone()
    }
}


#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LogicalPlanType {
    Aggregate,
    Any, // Any allows for any LogicalPlan variant to match on a search, think of it like a wildcard
    CrossJoin,
    Distinct,
    Join,
}

pub struct OptimizablePlanBackup<T>
where
    T: PlanVisitor,
{
    original_plan: LogicalPlan,
    search_criteria: Vec<LogicalPlanType>,
    // visitor: Box<dyn PlanVisitor<Error = DataFusionError>>,
    visitor: T,
    current_idx: usize,
    match_found: bool,
    left_plan_nodes: Vec<LogicalPlan>,
    match_plan_nodes: Vec<LogicalPlan>,
    right_plan_nodes: Vec<LogicalPlan>,
}

impl<T> OptimizablePlanBackup<T>
where
    T: PlanVisitor,
{
    pub fn new(plan: LogicalPlan, search_criteria: Vec<LogicalPlanType>, plan_visitor: T) -> Self {
        Self {
            original_plan: plan,
            search_criteria,
            // visitor: Box::new(EndOfNeedVisitor { original_plan: plan }),
            visitor: plan_visitor,
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
        let _find_result = self.original_plan.accept(&mut self.visitor);
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
        _replace: LogicalPlan,
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

impl<T> PlanVisitor for OptimizablePlanBackup<T>
where
    T: PlanVisitor,
{
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        // Ok lets see if we can match the search_criteria against these nodes
        if !self.match_found {
            // Push the current plan node into the left_plan_nodes, if a match is later determined it will be popped and moved
            self.left_plan_nodes.push(plan.clone());

            let cc = self.search_criteria.get(self.current_idx).unwrap();
            self.current_idx = match plan {
                LogicalPlan::Aggregate(_) => self.does_match(cc, &LogicalPlanType::Aggregate),
                LogicalPlan::CrossJoin(_) => self.does_match(cc, &LogicalPlanType::CrossJoin),
                LogicalPlan::Distinct(_) => self.does_match(cc, &LogicalPlanType::Distinct),
                LogicalPlan::Join(_) => self.does_match(cc, &LogicalPlanType::Join),
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

    fn post_visit(&mut self, _plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        Ok(true)
    }
}

impl<T> OptimizationStrategy for OptimizablePlanBackup<T>
    where T: PlanVisitor {

    fn optimize(&mut self, input_plan: &LogicalPlan) -> LogicalPlan {
        self.original_plan.clone()
    }

    fn rebuild(&mut self) -> LogicalPlan {
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
                    builder
                        .join(
                            (*join.right).clone(),
                            datafusion_expr::JoinType::Inner,
                            (left_cols, right_cols),
                            None,
                        )
                        .expect("invalid join node")
                }
                _ => panic!("Error, encountered: {:?}", p),
            }
        }

        builder.build().unwrap()

    }
}
