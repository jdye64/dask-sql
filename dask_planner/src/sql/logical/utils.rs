use std::sync::Arc;

use datafusion_python::{
    datafusion_common::DFField,
    datafusion_expr::{expr::Sort, utils::exprlist_to_fields, DdlStatement, Expr, LogicalPlan},
    expr::{projection::PyProjection, PyExpr},
};
use pyo3::{pyfunction, PyResult};

use super::{
    alter_schema::AlterSchemaPlanNode,
    alter_table::AlterTablePlanNode,
    analyze_table::AnalyzeTablePlanNode,
    create_catalog_schema::CreateCatalogSchemaPlanNode,
    create_experiment::CreateExperimentPlanNode,
    create_model::CreateModelPlanNode,
    create_table::CreateTablePlanNode,
    describe_model::DescribeModelPlanNode,
    drop_model::DropModelPlanNode,
    drop_schema::DropSchemaPlanNode,
    export_model::ExportModelPlanNode,
    predict_model::PredictModelPlanNode,
    show_columns::ShowColumnsPlanNode,
    show_models::ShowModelsPlanNode,
    show_schemas::ShowSchemasPlanNode,
    show_tables::ShowTablesPlanNode,
    use_schema::UseSchemaPlanNode,
};
use crate::{
    error::{DaskPlannerError, Result},
    sql::{
        exceptions::py_type_err,
        table::{table_from_logical_plan, DaskTable},
        types::{rel_data_type::RelDataType, rel_data_type_field::RelDataTypeField},
        DaskLogicalPlan,
    },
};

/// Convert a list of DataFusion Expr to PyExpr
pub fn py_expr_list(_input: &Arc<LogicalPlan>, expr: &[Expr]) -> PyResult<Vec<PyExpr>> {
    Ok(expr.iter().map(|e| PyExpr::from(e.clone())).collect())
}

/// Determines the name of the `Expr` instance by examining the LogicalPlan
pub fn column_name(expr: &Expr, plan: &LogicalPlan) -> Result<String> {
    let field = expr_to_field(expr, plan)?;
    Ok(field.qualified_column().flat_name())
}

/// Create a [DFField] representing an [Expr], given an input [LogicalPlan] to resolve against
pub fn expr_to_field(expr: &Expr, input_plan: &LogicalPlan) -> Result<DFField> {
    match expr {
        Expr::Sort(Sort { expr, .. }) => {
            // DataFusion does not support create_name for sort expressions (since they never
            // appear in projections) so we just delegate to the contained expression instead
            expr_to_field(expr, input_plan)
        }
        _ => {
            let fields =
                exprlist_to_fields(&[expr.clone()], input_plan).map_err(DaskPlannerError::from)?;
            Ok(fields[0].clone())
        }
    }
}

#[pyfunction]
pub fn py_column_name(expr: PyExpr, plan: DaskLogicalPlan) -> Result<String> {
    column_name(&expr.expr, &(*plan.plan()).clone())
}

#[pyfunction]
pub fn get_current_node_type(plan: DaskLogicalPlan) -> Result<String> {
    Ok(match &*plan.plan() {
        LogicalPlan::Dml(_) => "DataManipulationLanguage".to_string(),
        LogicalPlan::DescribeTable(_) => "DescribeTable".to_string(),
        LogicalPlan::Prepare(_) => "Prepare".to_string(),
        LogicalPlan::Distinct(_) => "Distinct".to_string(),
        LogicalPlan::Projection(_projection) => "Projection".to_string(),
        LogicalPlan::Filter(_filter) => "Filter".to_string(),
        LogicalPlan::Window(_window) => "Window".to_string(),
        LogicalPlan::Aggregate(_aggregate) => "Aggregate".to_string(),
        LogicalPlan::Sort(_sort) => "Sort".to_string(),
        LogicalPlan::Join(_join) => "Join".to_string(),
        LogicalPlan::CrossJoin(_cross_join) => "CrossJoin".to_string(),
        LogicalPlan::Repartition(_repartition) => "Repartition".to_string(),
        LogicalPlan::Union(_union) => "Union".to_string(),
        LogicalPlan::TableScan(_table_scan) => "TableScan".to_string(),
        LogicalPlan::EmptyRelation(_empty_relation) => "EmptyRelation".to_string(),
        LogicalPlan::Limit(_limit) => "Limit".to_string(),
        LogicalPlan::Ddl(ddl) => match ddl {
            DdlStatement::CreateExternalTable(_) => "CreateExternalTable".to_string(),
            DdlStatement::CreateCatalog(_) => "CreateCatalog".to_string(),
            DdlStatement::CreateCatalogSchema(_) => "CreateCatalogSchema".to_string(),
            DdlStatement::CreateMemoryTable(_) => "CreateMemoryTable".to_string(),
            DdlStatement::CreateView(_) => "CreateView".to_string(),
            DdlStatement::DropCatalogSchema(_) => "DropCatalogSchema".to_string(),
            DdlStatement::DropTable(_) => "DropTable".to_string(),
            DdlStatement::DropView(_) => "DropView".to_string(),
        },
        LogicalPlan::Values(_values) => "Values".to_string(),
        LogicalPlan::Explain(_explain) => "Explain".to_string(),
        LogicalPlan::Analyze(_analyze) => "Analyze".to_string(),
        LogicalPlan::Subquery(_sub_query) => "Subquery".to_string(),
        LogicalPlan::SubqueryAlias(_sqalias) => "SubqueryAlias".to_string(),
        LogicalPlan::Statement(_) => "Statement".to_string(),
        // Further examine and return the name that is a possible Dask-SQL Extension type
        LogicalPlan::Extension(extension) => {
            let node = extension.node.as_any();
            if node.downcast_ref::<CreateModelPlanNode>().is_some() {
                "CreateModel".to_string()
            } else if node.downcast_ref::<CreateExperimentPlanNode>().is_some() {
                "CreateExperiment".to_string()
            } else if node.downcast_ref::<CreateCatalogSchemaPlanNode>().is_some() {
                "CreateCatalogSchema".to_string()
            } else if node.downcast_ref::<CreateTablePlanNode>().is_some() {
                "CreateTable".to_string()
            } else if node.downcast_ref::<DropModelPlanNode>().is_some() {
                "DropModel".to_string()
            } else if node.downcast_ref::<PredictModelPlanNode>().is_some() {
                "PredictModel".to_string()
            } else if node.downcast_ref::<ExportModelPlanNode>().is_some() {
                "ExportModel".to_string()
            } else if node.downcast_ref::<DescribeModelPlanNode>().is_some() {
                "DescribeModel".to_string()
            } else if node.downcast_ref::<ShowSchemasPlanNode>().is_some() {
                "ShowSchemas".to_string()
            } else if node.downcast_ref::<ShowTablesPlanNode>().is_some() {
                "ShowTables".to_string()
            } else if node.downcast_ref::<ShowColumnsPlanNode>().is_some() {
                "ShowColumns".to_string()
            } else if node.downcast_ref::<ShowModelsPlanNode>().is_some() {
                "ShowModels".to_string()
            } else if node.downcast_ref::<DropSchemaPlanNode>().is_some() {
                "DropSchema".to_string()
            } else if node.downcast_ref::<UseSchemaPlanNode>().is_some() {
                "UseSchema".to_string()
            } else if node.downcast_ref::<AnalyzeTablePlanNode>().is_some() {
                "AnalyzeTable".to_string()
            } else if node.downcast_ref::<AlterTablePlanNode>().is_some() {
                "AlterTable".to_string()
            } else if node.downcast_ref::<AlterSchemaPlanNode>().is_some() {
                "AlterSchema".to_string()
            } else {
                // Default to generic `Extension`
                "Extension".to_string()
            }
        }
        LogicalPlan::Unnest(_unnest) => "Unnest".to_string(),
    })
}

#[pyfunction]
pub fn plan_to_table(plan: DaskLogicalPlan) -> PyResult<DaskTable> {
    match table_from_logical_plan(&plan.plan())? {
        Some(table) => Ok(table),
        None => Err(py_type_err(
            "Unable to compute DaskTable from DataFusion LogicalPlan",
        )),
    }
}

#[pyfunction]
pub fn row_type(plan: DaskLogicalPlan) -> PyResult<RelDataType> {
    match &*plan.plan() {
        LogicalPlan::Join(join) => {
            let mut lhs_fields: Vec<RelDataTypeField> = join
                .left
                .schema()
                .fields()
                .iter()
                .map(|f| RelDataTypeField::from(f, join.left.schema().as_ref()))
                .collect::<Result<Vec<_>>>()
                .map_err(py_type_err)?;

            let mut rhs_fields: Vec<RelDataTypeField> = join
                .right
                .schema()
                .fields()
                .iter()
                .map(|f| RelDataTypeField::from(f, join.right.schema().as_ref()))
                .collect::<Result<Vec<_>>>()
                .map_err(py_type_err)?;

            lhs_fields.append(&mut rhs_fields);
            Ok(RelDataType::new(false, lhs_fields))
        }
        LogicalPlan::Distinct(distinct) => {
            let schema = distinct.input.schema();
            let rel_fields: Vec<RelDataTypeField> = schema
                .fields()
                .iter()
                .map(|f| RelDataTypeField::from(f, schema.as_ref()))
                .collect::<Result<Vec<_>>>()
                .map_err(py_type_err)?;
            Ok(RelDataType::new(false, rel_fields))
        }
        _ => {
            let plan = (*plan.plan()).clone();
            let schema = plan.schema();
            let rel_fields: Vec<RelDataTypeField> = schema
                .fields()
                .iter()
                .map(|f| RelDataTypeField::from(f, schema.as_ref()))
                .collect::<Result<Vec<_>>>()
                .map_err(py_type_err)?;

            Ok(RelDataType::new(false, rel_fields))
        }
    }
}

#[pyfunction]
pub fn named_projects(projection: PyProjection) -> PyResult<Vec<(String, PyExpr)>> {
    let mut named: Vec<(String, PyExpr)> = Vec::new();
    for expression in projection.projection.expr {
        let py_expr: PyExpr = PyExpr::from(expression);
        for expr in PyProjection::projected_expressions(&py_expr) {
            match expr.expr {
                Expr::Alias(ex, name) => named.push((name.to_string(), PyExpr::from(*ex))),
                _ => {
                    if let Ok(name) = column_name(&expr.expr, &projection.projection.input) {
                        named.push((name, expr.clone()));
                    }
                }
            }
        }
    }
    Ok(named)
}

#[pyfunction]
pub fn distinct_agg(expr: PyExpr) -> PyResult<bool> {
    match expr.expr {
        Expr::AggregateFunction(funct) => Ok(funct.distinct),
        Expr::AggregateUDF { .. } => Ok(false),
        Expr::Alias(expr, _) => match expr.as_ref() {
            Expr::AggregateFunction(funct) => Ok(funct.distinct),
            Expr::AggregateUDF { .. } => Ok(false),
            _ => Err(py_type_err(
                "isDistinctAgg() - Non-aggregate expression encountered",
            )),
        },
        _ => Err(py_type_err(
            "getFilterExpr() - Non-aggregate expression encountered",
        )),
    }
}

/// Returns if a sort expressions is an ascending sort
#[pyfunction]
pub fn sort_ascending(expr: PyExpr) -> PyResult<bool> {
    match expr.expr {
        Expr::Sort(Sort { asc, .. }) => Ok(asc),
        _ => Err(py_type_err(format!(
            "Provided Expr {:?} is not a sort type",
            &expr.expr
        ))),
    }
}

/// Returns if nulls should be placed first in a sort expression
#[pyfunction]
pub fn sort_nulls_first(expr: PyExpr) -> PyResult<bool> {
    match expr.expr {
        Expr::Sort(Sort { nulls_first, .. }) => Ok(nulls_first),
        _ => Err(py_type_err(format!(
            "Provided Expr {:?} is not a sort type",
            &expr.expr
        ))),
    }
}

#[pyfunction]
pub fn get_filter_expr(expr: PyExpr) -> PyResult<Option<PyExpr>> {
    // TODO refactor to avoid duplication
    match &expr.expr {
        Expr::Alias(expr, _) => match expr.as_ref() {
            Expr::AggregateFunction(agg_function) => match &agg_function.filter {
                Some(filter) => Ok(Some(PyExpr::from(*filter.clone()))),
                None => Ok(None),
            },
            Expr::AggregateUDF { filter, .. } => match filter {
                Some(filter) => Ok(Some(PyExpr::from(*filter.clone()))),
                None => Ok(None),
            },
            _ => Err(py_type_err(
                "get_filter_expr() - Non-aggregate expression encountered",
            )),
        },
        Expr::AggregateFunction(agg_function) => match &agg_function.filter {
            Some(filter) => Ok(Some(PyExpr::from(*filter.clone()))),
            None => Ok(None),
        },
        Expr::AggregateUDF { filter, .. } => match filter {
            Some(filter) => Ok(Some(PyExpr::from(*filter.clone()))),
            None => Ok(None),
        },
        _ => Err(py_type_err(
            "get_filter_expr() - Non-aggregate expression encountered",
        )),
    }
}