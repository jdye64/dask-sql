from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql

from datafusion.expr import Explain


class ExplainPlugin(BaseRelPlugin):
    """
    Explain is used to explain the query with the EXPLAIN keyword
    """

    class_name = "Explain"

    def convert(self, explain: "Explain", context: "dask_sql.Context"):
        explain_strings = explain.plan_strings()
        return "\n".join(explain_strings)
