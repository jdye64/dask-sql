use datafusion_common::DataFusionError;
use datafusion_expr::LogicalPlan;


pub trait PlanFusion {
    fn fuse(&mut self,
        input_splits: Vec<LogicalPlan>,
        found_idxs: Vec<usize>,
        node: LogicalPlan) -> LogicalPlan;
}


/// Adds a node to the LogicalPlan during the fusion process before the found index
pub struct PreFoundIndexFusion {
    slices: Vec<LogicalPlan>,
    join_idxs: Vec<usize>,
}

impl PreFoundIndexFusion {

    pub fn new() -> Self {
        Self {
            slices: Vec::new(),
            join_idxs: Vec::new(),
        }
    }

    fn fuse_match_gap(&self, mut idx: usize, next_match_idx: usize, slices: &Vec<LogicalPlan>, cur_plan: LogicalPlan) -> LogicalPlan {

        let mut plan = cur_plan.clone();

        println!("fuse_match_gap - idx: {:?}, next_match_idx: {:?}", idx, next_match_idx);
        // next_match_idx will always be less because the order is sorted and in reverse
        while idx >= next_match_idx {
            // Make the result from the last operation the input of the slices[idx-1] node
            plan = slices[idx].clone().with_new_inputs(&[plan]).unwrap();
            if idx == 0 {
                break;
            }
            idx -= 1;
        }

        plan
    }
}

impl PlanFusion for PreFoundIndexFusion {

    fn fuse(&mut self,
        input_splits: Vec<LogicalPlan>,
        found_idxs: Vec<usize>,
        node: LogicalPlan) -> LogicalPlan {

        let mut plan = input_splits[0].clone();

        let mut has_match = false;
        let mut last_idx = 0;

        for idx in &found_idxs {
            println!("Found Idx: {:?}", idx);

            // This means that a find has already occurred
            if has_match {
                // If multiple matches exist. we need to fuse the "gap" between this match
                // and the next match in the loop
                plan = self.fuse_match_gap(last_idx - 1, *idx, &input_splits, plan);
            } else {
                has_match = true;
            }

            // Make the right side of the Vec the input of the inserted Node
            plan = node.with_new_inputs(&[input_splits[*idx].clone()]).unwrap();
            last_idx = *idx;
        }

        // Fuse the remaining bridge
        self.fuse_match_gap(last_idx - 1, 0, &input_splits, plan)
    }
}
