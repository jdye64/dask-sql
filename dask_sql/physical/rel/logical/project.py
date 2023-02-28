import logging
from typing import TYPE_CHECKING

from dask_planner.rust import RexType
from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter
from dask_sql.utils import new_temporary_column

if TYPE_CHECKING:
    from datafusion.expr import Projection

    import dask_sql

logger = logging.getLogger(__name__)


class DaskProjectPlugin(BaseRelPlugin):
    """
    A DaskProject is used to
    (a) apply expressions to the columns and
    (b) only select a subset of the columns
    """

    class_name = "Projection"

    def convert(self, proj: "Projection", context: "dask_sql.Context") -> DataContainer:
        # Get the input of the previous step
        (dc,) = self.assert_inputs(proj, 1, context)

        df = dc.df
        cc = dc.column_container

        column_names = []
        new_columns = {}
        new_mappings = {}

        # Collect all (new) columns this Projection will limit to
        for expr in proj.projections():
            column_names.append(expr.display_name())

            # Create a random name and assign it to the dataframe
            random_name = new_temporary_column(df)
            new_columns[random_name] = RexConverter.convert(
                proj, expr, dc, context=context
            )
            logger.debug(f"Adding a new column {expr.display_name()} out of {expr}")
            new_mappings[expr.display_name()] = random_name

        # Actually add the new columns
        if new_columns:
            df = df.assign(**new_columns)

        # and the new mappings
        for key, backend_column_name in new_mappings.items():
            cc = cc.add(key, backend_column_name)

        # Make sure the order is correct
        cc = cc.limit_to(column_names)

        cc = self.fix_column_to_row_type(cc, proj.schema())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, proj.schema())

        return dc
