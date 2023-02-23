pub mod aggregate;
pub mod alter_schema;
pub mod alter_table;
pub mod analyze_table;
pub mod create_catalog_schema;
pub mod create_experiment;
pub mod create_memory_table;
pub mod create_model;
pub mod create_table;
pub mod describe_model;
pub mod drop_model;
pub mod drop_schema;
pub mod drop_table;
pub mod explain;
pub mod export_model;
pub mod filter;
pub mod join;
pub mod limit;
pub mod predict_model;
pub mod projection;
pub mod repartition_by;
pub mod show_columns;
pub mod show_models;
pub mod show_schema;
pub mod show_tables;
pub mod sort;
pub mod subquery_alias;
pub mod table_scan;
pub mod use_schema;
pub mod window;

use datafusion_expr::LogicalPlan;

// use self::{
//     alter_schema::AlterSchemaPlanNode,
//     alter_table::AlterTablePlanNode,
//     analyze_table::AnalyzeTablePlanNode,
//     create_catalog_schema::CreateCatalogSchemaPlanNode,
//     create_experiment::CreateExperimentPlanNode,
//     create_model::CreateModelPlanNode,
//     create_table::CreateTablePlanNode,
//     describe_model::DescribeModelPlanNode,
//     drop_model::DropModelPlanNode,
//     drop_schema::DropSchemaPlanNode,
//     export_model::ExportModelPlanNode,
//     predict_model::PredictModelPlanNode,
//     show_columns::ShowColumnsPlanNode,
//     show_models::ShowModelsPlanNode,
//     show_schema::ShowSchemasPlanNode,
//     show_tables::ShowTablesPlanNode,
//     use_schema::UseSchemaPlanNode,
// };
use crate::sql::exceptions::py_type_err;

// /// Convert a LogicalPlan to a Python equivalent type
// fn to_py_plan<T: TryFrom<LogicalPlan, Error = PyErr>>(
//     current_node: Option<&LogicalPlan>,
// ) -> PyResult<T> {
//     match current_node {
//         Some(plan) => plan.clone().try_into(),
//         _ => Err(py_type_err("current_node was None")),
//     }
// }

// #[pymethods]
// impl PyLogicalPlan {
//     /// LogicalPlan::Aggregate as PyAggregate
//     pub fn aggregate(&self) -> PyResult<aggregate::PyAggregate> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::EmptyRelation as PyEmptyRelation
//     pub fn empty_relation(&self) -> PyResult<empty_relation::PyEmptyRelation> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Explain as PyExplain
//     pub fn explain(&self) -> PyResult<explain::PyExplain> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Filter as PyFilter
//     pub fn filter(&self) -> PyResult<filter::PyFilter> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Join as PyJoin
//     pub fn join(&self) -> PyResult<join::PyJoin> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Limit as PyLimit
//     pub fn limit(&self) -> PyResult<limit::PyLimit> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Projection as PyProjection
//     pub fn projection(&self) -> PyResult<projection::PyProjection> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Sort as PySort
//     pub fn sort(&self) -> PyResult<sort::PySort> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::SubqueryAlias as PySubqueryAlias
//     pub fn subquery_alias(&self) -> PyResult<subquery_alias::PySubqueryAlias> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Window as PyWindow
//     pub fn window(&self) -> PyResult<window::PyWindow> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::TableScan as PyTableScan
//     pub fn table_scan(&self) -> PyResult<table_scan::PyTableScan> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::CreateMemoryTable as PyCreateMemoryTable
//     pub fn create_memory_table(&self) -> PyResult<create_memory_table::PyCreateMemoryTable> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::CreateModel as PyCreateModel
//     pub fn create_model(&self) -> PyResult<create_model::PyCreateModel> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::CreateExperiment as PyCreateExperiment
//     pub fn create_experiment(&self) -> PyResult<create_experiment::PyCreateExperiment> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::DropTable as DropTable
//     pub fn drop_table(&self) -> PyResult<drop_table::PyDropTable> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::DropModel as DropModel
//     pub fn drop_model(&self) -> PyResult<drop_model::PyDropModel> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::ShowSchemas as PyShowSchemas
//     pub fn show_schemas(&self) -> PyResult<show_schema::PyShowSchema> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Repartition as PyRepartitionBy
//     pub fn repartition_by(&self) -> PyResult<repartition_by::PyRepartitionBy> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::ShowTables as PyShowTables
//     pub fn show_tables(&self) -> PyResult<show_tables::PyShowTables> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::CreateTable as PyCreateTable
//     pub fn create_table(&self) -> PyResult<create_table::PyCreateTable> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::PredictModel as PyPredictModel
//     pub fn predict_model(&self) -> PyResult<predict_model::PyPredictModel> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::DescribeModel as PyDescribeModel
//     pub fn describe_model(&self) -> PyResult<describe_model::PyDescribeModel> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::ExportModel as PyExportModel
//     pub fn export_model(&self) -> PyResult<export_model::PyExportModel> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::ShowColumns as PyShowColumns
//     pub fn show_columns(&self) -> PyResult<show_columns::PyShowColumns> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     pub fn show_models(&self) -> PyResult<show_models::PyShowModels> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::ShowColumns as PyShowColumns
//     pub fn analyze_table(&self) -> PyResult<analyze_table::PyAnalyzeTable> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::CreateCatalogSchema as PyCreateCatalogSchema
//     pub fn create_catalog_schema(&self) -> PyResult<create_catalog_schema::PyCreateCatalogSchema> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::DropSchema as PyDropSchema
//     pub fn drop_schema(&self) -> PyResult<drop_schema::PyDropSchema> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::UseSchema as PyUseSchema
//     pub fn use_schema(&self) -> PyResult<use_schema::PyUseSchema> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::AlterTable as PyAlterTable
//     pub fn alter_table(&self) -> PyResult<alter_table::PyAlterTable> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// LogicalPlan::Extension::AlterSchema as PyAlterSchema
//     pub fn alter_schema(&self) -> PyResult<alter_schema::PyAlterSchema> {
//         to_py_plan(self.current_node.as_ref())
//     }

//     /// If the LogicalPlan represents access to a Table that instance is returned
//     /// otherwise None is returned
//     #[pyo3(name = "getTable")]
//     pub fn table(&mut self) -> PyResult<table::DaskTable> {
//         match table::table_from_logical_plan(&self.current_node())? {
//             Some(table) => Ok(table),
//             None => Err(py_type_err(
//                 "Unable to compute DaskTable from DataFusion LogicalPlan",
//             )),
//         }
//     }

//     #[pyo3(name = "getCurrentNodeSchemaName")]
//     pub fn get_current_node_schema_name(&self) -> PyResult<&str> {
//         match &self.current_node {
//             Some(e) => {
//                 let _sch: &DFSchemaRef = e.schema();
//                 //TODO: Where can I actually get this in the context of the running query?
//                 Ok("root")
//             }
//             None => Err(py_type_err(DataFusionError::Plan(format!(
//                 "Current schema not found. Defaulting to {:?}",
//                 "root"
//             )))),
//         }
//     }

//     #[pyo3(name = "getCurrentNodeTableName")]
//     pub fn get_current_node_table_name(&mut self) -> PyResult<String> {
//         match self.table() {
//             Ok(dask_table) => Ok(dask_table.table_name),
//             Err(_e) => Err(py_type_err("Unable to determine current node table name")),
//         }
//     }


//     #[pyo3(name = "getRowType")]
//     pub fn row_type(&self) -> PyResult<RelDataType> {
//         match &self.original_plan {
//             LogicalPlan::Join(join) => {
//                 let mut lhs_fields: Vec<RelDataTypeField> = join
//                     .left
//                     .schema()
//                     .fields()
//                     .iter()
//                     .map(|f| RelDataTypeField::from(f, join.left.schema().as_ref()))
//                     .collect::<Result<Vec<_>>>()
//                     .map_err(py_type_err)?;

//                 let mut rhs_fields: Vec<RelDataTypeField> = join
//                     .right
//                     .schema()
//                     .fields()
//                     .iter()
//                     .map(|f| RelDataTypeField::from(f, join.right.schema().as_ref()))
//                     .collect::<Result<Vec<_>>>()
//                     .map_err(py_type_err)?;

//                 lhs_fields.append(&mut rhs_fields);
//                 Ok(RelDataType::new(false, lhs_fields))
//             }
//             LogicalPlan::Distinct(distinct) => {
//                 let schema = distinct.input.schema();
//                 let rel_fields: Vec<RelDataTypeField> = schema
//                     .fields()
//                     .iter()
//                     .map(|f| RelDataTypeField::from(f, schema.as_ref()))
//                     .collect::<Result<Vec<_>>>()
//                     .map_err(py_type_err)?;
//                 Ok(RelDataType::new(false, rel_fields))
//             }
//             _ => {
//                 let schema = self.original_plan.schema();
//                 let rel_fields: Vec<RelDataTypeField> = schema
//                     .fields()
//                     .iter()
//                     .map(|f| RelDataTypeField::from(f, schema.as_ref()))
//                     .collect::<Result<Vec<_>>>()
//                     .map_err(py_type_err)?;
//                 Ok(RelDataType::new(false, rel_fields))
//             }
//         }
//     }
// }
