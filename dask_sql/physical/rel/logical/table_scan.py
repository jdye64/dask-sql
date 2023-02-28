import logging
import operator
from functools import reduce
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rel.logical.filter import filter_or_scalar
from dask_sql.physical.rex import RexConverter

if TYPE_CHECKING:
    from datafusion.expr import TableScan

    import dask_sql

logger = logging.getLogger(__name__)


class DaskTableScanPlugin(BaseRelPlugin):
    """
    A DaskTableScan is the main ingredient: it will get the data
    from the database. It is always used, when the SQL looks like

        SELECT .... FROM table ....

    We need to get the dask dataframe from the registered
    tables and return the requested columns from it.
    """

    class_name = "TableScan"

    def convert(
        self,
        table_scan: "TableScan",
        context: "dask_sql.Context",
    ) -> DataContainer:
        table_name = (
            table_scan.table_name()
        )  # FQN of the Table in the form `catalog.schema.table`

        dc = context.schema[context.DEFAULT_SCHEMA_NAME].tables[table_name]

        # Apply filter before projections since filter columns may not be in projections
        dc = self._apply_filters(table_scan, dc, context)
        dc = self._apply_projections(table_scan, dc)

        schema = table_scan.schema()

        cc = dc.column_container
        cc = self.fix_column_to_row_type(cc, schema)
        dc = DataContainer(dc.df, cc)
        dc = self.fix_dtype_to_row_type(dc, schema)
        return dc

    def _apply_projections(self, table_scan, dc):
        # If the 'TableScan' instance contains projected columns only retrieve those columns
        # otherwise get all projected columns from the 'Projection' instance
        df = dc.df
        cc = dc.column_container
        if len(table_scan.projections()):
            field_specifications = [
                col_name for _, col_name in table_scan.projections()
            ]
            df = df[field_specifications]
            cc = cc.limit_to(field_specifications)
            return DataContainer(df, cc)

        # Return DataContainer as is with all columns present
        return dc

    def _apply_filters(self, table_scan, dc, context):
        df = dc.df
        cc = dc.column_container
        filters = table_scan.filters()
        # All partial filters here are applied in conjunction (&)
        if filters:
            df_condition = reduce(
                operator.and_,
                [
                    RexConverter.convert(table_scan, rex, dc, context=context)
                    for rex in filters
                ],
            )
            df = filter_or_scalar(df, df_condition)

        return DataContainer(df, cc)
