import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner import LogicalPlan

from datafusion.expr import CreateView

logger = logging.getLogger(__name__)


class CreateViewPlugin(BaseRelPlugin):
    """
    Create a view from the given SELECT query
    and register it at the context.
    The SQL call looks like

        CREATE VIEW <table-name> AS
            <some select query>

    It sends the select query through the normal parsing
    and optimization and conversation before registering it.

    Using this SQL is equivalent to just doing

        df = context.sql("<select query>")
        context.create_table(<table-name>, df)

    but can also be used without writing a single line of code.
    Nothing is returned.
    """

    class_name = "CreateView"

    def convert(
        self, create_view: "CreateView", context: "dask_sql.Context"
    ) -> DataContainer:

        qualified_table_name = create_view.name()
        *schema_name, table_name = qualified_table_name.split(".")

        if len(schema_name) > 1:
            raise RuntimeError(
                f"Expected unqualified or fully qualified table name, got {qualified_table_name}."
            )

        schema_name = context.schema_name if not schema_name else schema_name[0]

        if schema_name not in context.schema:
            raise RuntimeError(f"A schema with the name {schema_name} is not present.")
        if table_name in context.schema[schema_name].tables:
            if not create_view.or_replace():
                raise RuntimeError(
                    f"A view with the name {table_name} is already present."
                )

        input_rel = create_view.input()

        # Ensure that only a single Input LogicalPlan is assigned to the input_rel
        if isinstance(input_rel, list):
            input_rel = input_rel[0]

        # This is always False now, since this logic is ONLY invoked for a CREATE VIEW AS statement
        persist = False

        logger.debug(
            f"Creating new view with name {qualified_table_name} and logical plan {input_rel}"
        )

        context.create_table(
            table_name,
            context._compute_table_from_rel(input_rel),
            persist=persist,
            schema_name=schema_name,
        )
