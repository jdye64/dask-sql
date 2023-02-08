use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, PlanVisitor};


/// Dissects a plan into a Vec of isolated `LogicalPlan` instances
pub trait Dissector : PlanVisitor {

    fn dissect(&mut self, input_plan: &LogicalPlan) -> Vec<LogicalPlan>;
}


pub struct JoinDissector {
    pub(crate) slices: Vec<LogicalPlan>,
    current_idx: usize,
    pub(crate) join_idxs: Vec<usize>,
}

impl JoinDissector {

    pub fn new() -> Self {
        Self {
            slices: Vec::new(),
            current_idx: 0,
            join_idxs: Vec::new(),
        }
    }
}

impl Dissector for JoinDissector {

    fn dissect(&mut self, input_plan: &LogicalPlan) -> Vec<LogicalPlan> {
        let result = input_plan.accept(self);
        self.slices.clone()
    }
}

impl PlanVisitor for JoinDissector {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {

        self.slices.push(plan.clone());
        match plan {
            LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_) => self.join_idxs.push(self.current_idx),
            _ => ()
        }
        self.current_idx += 1;

        Ok(true)
    }

    // Update the last_seen_node_map with the last used location of each Column
    fn post_visit(&mut self, _plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        Ok(true)
    }
}
