use std::collections::HashMap;

use colored::Colorize;
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, PlanVisitor};

/// Represents an optimization routine that searches for the last necessary position of a Expr.
/// Ex: Find the last location that a column is needed so that it can be removed and not included
/// in subsequent shuffles and downstream operations un-necessarily
/// Ex: Push joins down further into the execution stack
pub struct EndOfNeedVisitor {
    original_plan: LogicalPlan,
    // While traversing the plan keeps track of the last node that the column was seen at.
    last_seen_node_map: HashMap<String, LogicalPlan>,
}

impl EndOfNeedVisitor {
    fn extract_column_name(&self, expr: &Expr) -> Vec<String> {
        match expr {
            Expr::Column(column) => vec![column.name.clone()],
            Expr::AggregateFunction(agg_func) => {
                let mut col_names: Vec<String> = Vec::new();
                for expr in &agg_func.args {
                    col_names.append(&mut self.extract_column_name(expr));
                }
                col_names
            }
            _ => panic!("don't know what to do?: {:?}", expr),
        }
    }
}

impl PlanVisitor for EndOfNeedVisitor {
    type Error = DataFusionError;

    // Since we are looking for the last place a column is needed pre_visit doesn't do
    // what we need. Instead we use post_visit.
    fn pre_visit(&mut self, _plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        Ok(true)
    }

    // Update the last_seen_node_map with the last used location of each Column
    fn post_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        let col_names: Vec<String> = match plan {
            // LogicalPlan::Aggregate(agg) => agg.
            // LogicalPlan::Distinct(distinct) => distinct.
            LogicalPlan::Projection(projection) => projection.schema.field_names(),
            LogicalPlan::Filter(filter) => self.extract_column_name(&filter.predicate),
            LogicalPlan::TableScan(scan) => scan.projected_schema.field_names(),
            LogicalPlan::Aggregate(agg) => {
                let mut col_names: Vec<String> = Vec::new();
                for ag_ex in &agg.aggr_expr {
                    col_names.append(&mut self.extract_column_name(ag_ex));
                }

                for grp_expr in &agg.group_expr {
                    col_names.append(&mut self.extract_column_name(grp_expr));
                }

                col_names
            }
            LogicalPlan::Join(_join) => vec![], // I think it makes sense to just let the post_visit continue and return nothing for explicit joins.
            _ => panic!("something: {:?}", plan),
        };

        // Insert all the columns into the last_seen_map
        for n in col_names {
            self.last_seen_node_map.insert(n, plan.clone());
        }

        Ok(true)
    }
}

impl OptimizationStrategy for EndOfNeedVisitor {
    fn optimize(&mut self) -> LogicalPlan {
        // Check the last time that columns were seen
        for (key, value) in &self.last_seen_node_map {
            println!("{:?} / {:?}", key, value);
        }

        // TODO: just returning original plan for now
        self.original_plan.clone()
    }

    fn rebuild(&mut self) -> LogicalPlan {
        todo!()
    }
}
