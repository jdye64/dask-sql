use pyo3::prelude::*;

mod dialect;
mod error;
mod expression;
mod parser;
mod sql;

use datafusion_python::sql::logical::PyLogicalPlan;

/// Low-level DataFusion internal package.
///
/// The higher-level public API is defined in pure python files under the
/// dask_planner directory.
#[pymodule]
#[pyo3(name = "rust")]
fn rust(py: Python, m: &PyModule) -> PyResult<()> {
    // Register the python classes
    m.add_class::<expression::PyExpr>()?;
    m.add_class::<sql::DaskSQLContext>()?;
    m.add_class::<sql::types::SqlTypeName>()?;
    m.add_class::<sql::types::RexType>()?;
    m.add_class::<sql::types::DaskTypeMap>()?;
    m.add_class::<sql::types::rel_data_type::RelDataType>()?;
    m.add_class::<sql::statement::PyStatement>()?;
    m.add_class::<sql::schema::DaskSchema>()?;
    m.add_class::<sql::table::DaskTable>()?;
    m.add_class::<sql::function::DaskFunction>()?;
    m.add_class::<sql::table::DaskStatistics>()?;
    m.add_class::<PyLogicalPlan>()?;

    // Register the SQL componenets
    m.add_class::<sql::logical::analyze_table::PyAnalyzeTable>()?;
    m.add_class::<sql::logical::aggregate::PyAggregate>()?;
    m.add_class::<sql::logical::alter_schema::PyAlterSchema>()?;
    m.add_class::<sql::logical::alter_table::PyAlterTable>()?;
    m.add_class::<sql::logical::create_catalog_schema::PyCreateCatalogSchema>()?;
    m.add_class::<sql::logical::create_experiment::PyCreateExperiment>()?;
    m.add_class::<sql::logical::describe_model::PyDescribeModel>()?;
    m.add_class::<sql::logical::drop_model::PyDropModel>()?;
    m.add_class::<sql::logical::drop_schema::PyDropSchema>()?;
    m.add_class::<sql::logical::explain::PyExplain>()?;
    m.add_class::<sql::logical::export_model::PyExportModel>()?;
    m.add_class::<sql::logical::filter::PyFilter>()?;
    m.add_class::<sql::logical::join::PyJoin>()?;
    m.add_class::<sql::logical::predict_model::PyPredictModel>()?;
    m.add_class::<sql::logical::show_columns::PyShowColumns>()?;
    m.add_class::<sql::logical::show_models::PyShowModels>()?;
    m.add_class::<sql::logical::show_schema::PyShowSchema>()?;
    m.add_class::<sql::logical::show_tables::PyShowTables>()?;
    m.add_class::<sql::logical::sort::PySort>()?;
    m.add_class::<sql::logical::use_schema::PyUseSchema>()?;
    m.add_class::<sql::logical::window::PyWindow>()?;
    m.add_class::<sql::logical::create_table::PyCreateTable>()?;

    // Exceptions
    m.add(
        "DFParsingException",
        py.get_type::<sql::exceptions::ParsingException>(),
    )?;
    m.add(
        "DFOptimizationException",
        py.get_type::<sql::exceptions::OptimizationException>(),
    )?;

    Ok(())
}
