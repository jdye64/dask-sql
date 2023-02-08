use std::collections::HashMap;

use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, PlanVisitor};

use super::OptimizationStrategy;


#[derive(Default)]
/// Traverses a `LogicalPlan` instance and records the index of the last location
/// in the plan that a `Column` is used
pub struct ColumnIndexBuilder {
    // While traversing the plan keeps track of the last node that the column was seen at.
    last_seen_node_map: HashMap<String, LogicalPlan>,
}

impl ColumnIndexBuilder {

    pub fn new() -> Self {
        Self {
            last_seen_node_map: HashMap::new()
        }
    }

    // fn extract_column_name(&self, expr: &Expr) -> String {
    //     match expr {
    //         Expr::Column(column) => column.name.clone(),
    //         _ => panic!("don't know what to do?: {:?}", expr),
    //     }
    // }
}

impl PlanVisitor for ColumnIndexBuilder {
    type Error = DataFusionError;

    // Since we are looking for the last place a column is needed pre_visit doesn't do
    // what we need. Instead we use post_visit (AKA BOTTOM->UP).
    fn pre_visit(&mut self, _plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        Ok(true)
    }

    // Update the last_seen_node_map with the last used location of each Column
    fn post_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        println!("Post_Visit: {:?}", plan);
        // let col_name: String = match plan {
        //     LogicalPlan::Projection(projection) => projection.schema.field_names(),
        //     LogicalPlan::Filter(filter) => self.extract_column_name(&filter.predicate),
        //     LogicalPlan::TableScan(scan) => scan.projected_schema.field_names(),
        //     LogicalPlan::Aggregate(agg) => {
        //         let mut col_names: Vec<String> = Vec::new();
        //         for ag_ex in &agg.aggr_expr {
        //             col_names.append(&mut self.extract_column_name(ag_ex));
        //         }

        //         for grp_expr in &agg.group_expr {
        //             col_names.append(&mut self.extract_column_name(grp_expr));
        //         }

        //         col_names
        //     }
        //     LogicalPlan::Join(_join) => vec![], // I think it makes sense to just let the post_visit continue and return nothing for explicit joins.
        //     _ => panic!("something: {:?}", plan),
        // };

        // // Insert all the columns into the last_seen_map
        // for n in col_names {
        //     self.last_seen_node_map.insert(n, plan.clone());
        // }

        Ok(true)
    }
}

impl OptimizationStrategy for ColumnIndexBuilder {
    fn optimize(&mut self, input_plan: &LogicalPlan) -> LogicalPlan {

        match input_plan.accept(self) {
            Ok(b) => (),
            Err(e) => panic!("Error in ColumnIndexBuild: {:?}", e),
        }

        // Check the last time that columns were seen
        for (key, value) in &self.last_seen_node_map {
            println!("{:?} / {:?}", key, value);
        }

        input_plan.clone()
    }

    fn rebuild(&mut self) -> LogicalPlan {
        todo!()
    }
}
