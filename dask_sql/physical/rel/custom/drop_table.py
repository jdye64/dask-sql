import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql

from datafusion.expr import DropTable

logger = logging.getLogger(__name__)


class DropTablePlugin(BaseRelPlugin):
    """
    Drop a table with given name.
    The SQL call looks like

        DROP TABLE <table-name>
    """

    class_name = "DropTable"

    def convert(
        self, drop_table: "DropTable", context: "dask_sql.Context"
    ) -> DataContainer:

        qualified_table_name = drop_table.name()
        *schema_name, table_name = qualified_table_name.split(".")

        if len(schema_name) > 1:
            raise RuntimeError(
                f"Expected unqualified or fully qualified table name, got {qualified_table_name}."
            )

        schema_name = context.schema_name if not schema_name else schema_name[0]

        if (
            schema_name not in context.schema
            or table_name not in context.schema[schema_name].tables
        ):
            if not drop_table.if_exists():
                raise RuntimeError(
                    f"A table with the name {qualified_table_name} is not present."
                )
            else:
                return

        context.drop_table(table_name, schema_name=schema_name)
