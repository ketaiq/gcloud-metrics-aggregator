import ast
import csv
import os
import pandas as pd
import logging
import warnings


class AggregateHandler:
    COL_KPI_INDEX = "kpi_index"

    def __init__(
        self,
        handler_name: str,
        metric_index: int,
        metric_name: str,
        df_kpi_map: pd.DataFrame,
        df_metric: pd.DataFrame,
        enforce_existing_aggregations: bool,
    ):
        self.metric_index = metric_index
        self.metric_name = metric_name
        self.df_kpi_map = df_kpi_map
        self.df_metric = df_metric
        self.enforce_existing_aggregations = enforce_existing_aggregations
        self.path_aggregations = os.path.join("aggregations", f"{handler_name}.csv")
        self.aggregations = pd.read_csv(self.path_aggregations)
        self.aggregations["groups"] = self.aggregations["groups"].apply(
            ast.literal_eval
        )
        self.aggregations["methods"] = self.aggregations["methods"].apply(
            ast.literal_eval
        )

    def record_new_aggregation(
        self, index: int, name: str, groups: list, methods: list
    ):
        """Record a new aggregation in the file."""
        with open(self.path_aggregations, mode="a", newline="") as f:
            csv.writer(f).writerow([index, name, groups, methods])

    def apply_existing_aggregation(self, aggregation: pd.Series) -> pd.DataFrame:
        """Apply an existing aggregation record."""
        aggregation = aggregation.iloc[0]
        groups = aggregation["groups"]
        methods = aggregation["methods"]
        if groups and methods:
            return self.aggregate_with_groups(groups, methods)
        elif groups and len(groups) == 1:
            return self.df_metric.rename(columns=self.gen_map_columns(groups[0]))
        elif groups and len(groups) > 1:
            msg = f"Fail to transform {self.metric_name} because of more than one groups: {groups}!"
            logging.error(msg)
            raise ValueError(msg)
        elif methods:
            return self.apply_aggregation(self.df_metric, methods)
        else:
            return self.df_metric.rename(columns=self.gen_map_columns())

    def aggregate_kpis(self):
        logging.warning(
            "Method aggregate_kpis should be overridden in a concrete handler!"
        )

    def apply_aggregation(
        self, df_metric_to_agg: pd.DataFrame, agg_methods: list = None
    ) -> pd.DataFrame:
        if agg_methods is None:
            # default aggregation methods
            agg_methods = [
                "min",
                "max",
                "mean",
                "median",
                AggregateHandler.first_quartile,
                AggregateHandler.third_quartile,
            ]
        else:
            for name, func in {
                "first_quartile": AggregateHandler.first_quartile,
                "third_quartile": AggregateHandler.third_quartile,
            }.items():
                if name in agg_methods:
                    index = agg_methods.index(name)
                    agg_methods[index] = func

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            return df_metric_to_agg.agg(agg_methods, axis=1)

    def aggregate_with_groups(self, group_columns: list, agg_methods: list = None):
        df_kpi_indices = self.df_kpi_map.groupby(group_columns).agg(
            lambda s: s.to_list()
        )
        list_df_agg_metric = []
        for index in df_kpi_indices.index:
            kpi_indices_to_agg = df_kpi_indices.loc[
                index, AggregateHandler.COL_KPI_INDEX
            ]
            if self.is_distribution():
                for m in ["count", "mean", "sum_of_squared_deviation"]:
                    kpi_columns_to_agg = [f"kpi-{i}-{m}" for i in kpi_indices_to_agg]
                    df_metric_to_agg = self.df_metric[kpi_columns_to_agg]
                    column_prefix = (
                        AggregateHandler.gen_column_prefix(index, df_kpi_indices)
                        + "distribut_"
                        + m
                        + "-"
                    )
                    list_df_agg_metric.append(
                        self.apply_aggregation(
                            df_metric_to_agg, agg_methods
                        ).add_prefix(column_prefix)
                    )
            else:
                kpi_columns_to_agg = [f"kpi-{i}-value" for i in kpi_indices_to_agg]
                df_metric_to_agg = self.df_metric[kpi_columns_to_agg]

                list_df_agg_metric.append(
                    self.apply_aggregation(df_metric_to_agg, agg_methods).add_prefix(
                        AggregateHandler.gen_column_prefix(index, df_kpi_indices)
                    )
                )
        return pd.concat(list_df_agg_metric, axis=1)

    def gen_map_columns(self, target_column: str = None) -> dict:
        if len(self.df_kpi_map) == 1:
            return {"kpi-0-value": "kpi-value"}
        df_kpi_map = self.df_kpi_map.set_index("kpi_index")
        map_columns = {}
        for kpi_index in df_kpi_map.index:
            original_column_name = f"kpi-{kpi_index}-value"
            # find distinguished words from KPI map as KPI names
            if target_column is None:
                map_columns[original_column_name] = "-".join(
                    list(
                        (
                            name.upper() + "-" + str(value).lower()
                            for name, value in df_kpi_map.loc[kpi_index]
                            .to_dict()
                            .items()
                        )
                    )
                )
            else:
                map_columns[original_column_name] = df_kpi_map.loc[
                    kpi_index, target_column
                ]
        return map_columns

    @staticmethod
    def first_quartile(series: pd.Series):
        return series.quantile(0.25)

    @staticmethod
    def third_quartile(series: pd.Series):
        return series.quantile(0.75)

    @staticmethod
    def percentile(n):
        def percentile_(series: pd.Series):
            return series.quantile(n)

        n_int = int(n * 100)
        percentile_.__name__ = f"percentile_{n_int}"
        return percentile_

    def is_distribution(self) -> bool:
        for col in self.df_metric.columns:
            if "sum_of_squared_deviation" in col:
                return True
        return False

    @staticmethod
    def gen_column_prefix(index, df_kpi_indices: pd.DataFrame) -> str:
        if type(index) is tuple:
            return (
                "-".join(
                    list(
                        (
                            name.upper() + "-" + str(value).lower()
                            for name, value in zip(df_kpi_indices.index.names, index)
                        )
                    )
                )
                + "-"
            )
        else:
            return df_kpi_indices.index.name.upper() + "-" + str(index).lower() + "-"
