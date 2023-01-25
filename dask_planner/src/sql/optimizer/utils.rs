use std::collections::LinkedList;

use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, PlanVisitor};

#[derive(Debug)]
pub struct OptimizablePlan {
    original_plan: LogicalPlan,
    flex_plan: LinkedList<LogicalPlan>
}

impl OptimizablePlan {
    pub fn new(plan: LogicalPlan) -> Self {
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
    }
}

impl PlanVisitor for OptimizablePlan {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        // If the plan contains an unsupported Node type we flag the plan as un-optimizable here
        match plan {
            LogicalPlan::Explain(..) => Ok(false),
            _ => Ok(true),
        }
    }

    fn post_visit(&mut self, _plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        Ok(true)
    }
}



#[cfg(test)]
mod test {
    use arrow::datatypes::DataType;
    use datafusion_expr::{Signature, TypeSignature, Volatility};

    use crate::sql::generate_signatures;

    #[test]
    fn test_generate_signatures() {
        let sig = generate_signatures(vec![
            vec![DataType::Int64, DataType::Float64],
            vec![DataType::Utf8, DataType::Int64],
        ]);
        let expected = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Int64]),
            ],
            Volatility::Immutable,
        );
        assert_eq!(sig, expected);
    }
}
