import logging
from typing import TYPE_CHECKING, Any, Union

import dask.dataframe as dd

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.utils import LoggableDataFrame, Pluggable

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import Expression, LogicalPlan

logger = logging.getLogger(__name__)

# Mapping of https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html variants to Python plugin
_EXPR_VARIANT_TO_PLUGIN = {
    "Alias": "RexAlias",
    "Column": "InputRef",
    "ScalarVariable": "RexLiteral",
    "Literal": "RexLiteral",
    "BinaryExpr": "RexCall",
    "Like": "RexCall",
    "ILike": "RexCall",
    "SimilarTo": "RexCall",
    "Not": "RexCall",
    "IsNotNull": "RexCall",
    "IsNull": "RexCall",
    "IsTrue": "RexCall",
    "IsFalse": "RexCall",
    "IsUnknown": "RexCall",
    "IsNotTrue": "RexCall",
    "IsNotFalse": "RexCall",
    "IsNotUnknown": "RexCall",
    "Negative": "RexCall",
    "GetIndexedField": "RexCall",
    "Between": "RexCall",
    "Case": "RexCall",
    "Cast": "RexCall",
    "TryCast": "RexCall",
    "Sort": "RexCall",
    "ScalarFunction": "RexCall",
    "ScalarUDF": "RexCall",
    "AggregateFunction": "RexCall",
    "WindowFunction": "RexCall",
    "AggregateUDF": "RexCall",
    "InList": "RexCall",
    "Exists": "RexCall",
    "InSubquery": "RexCall",
    "ScalarSubquery": "ScalarSubquery",
    "Wildcard": "RexCall",
    "QualifiedWildcard": "RexCall",
    "GroupingSet": "RexCall",
    "Placeholder": "RexCall",
}


class RexConverter(Pluggable):
    """
    Helper to convert from rex to a python expression

    This class stores plugins which can convert from RexNodes to
    python expression (single values or dask dataframe columns).
    The stored plugins are assumed to have a class attribute "class_name"
    to control, which java classes they can convert
    and they are expected to have a convert (instance) method
    in the form

        def convert(self, rex, df)

    to do the actual conversion.
    """

    @classmethod
    def add_plugin_class(cls, plugin_class: BaseRexPlugin, replace=True):
        """Convenience function to add a class directly to the plugins"""
        logger.debug(f"Registering REX plugin for {plugin_class.class_name}")
        cls.add_plugin(plugin_class.class_name, plugin_class(), replace=replace)

    @classmethod
    def convert(
        cls,
        rel: "LogicalPlan",
        rex: "Expression",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Union[dd.DataFrame, Any]:
        """
        Convert the given Expression
        into a python expression (a dask dataframe)
        using the stored plugins and the dictionary of
        registered dask tables.
        """
        expr_type = _EXPR_VARIANT_TO_PLUGIN[type(rex.to_variant()).__name__]

        try:
            plugin_instance = cls.get_plugin(expr_type)
        except KeyError:  # pragma: no cover
            raise NotImplementedError(
                f"No conversion for class {expr_type} available (yet)."
            )

        logger.debug(
            f"Processing REX {rex} using {plugin_instance.__class__.__name__}..."
        )

        df = plugin_instance.convert(rel, rex, dc, context=context)
        logger.debug(f"Processed REX {rex} into {LoggableDataFrame(df)}")
        return df
