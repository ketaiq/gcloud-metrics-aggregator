import logging
import pandas as pd
from app.agg.aggregate_handler import AggregateHandler


class PrometheusAggHandler(AggregateHandler):
    USELESS_COLUMNS = {"uid"}

    def __init__(
        self,
        metric_index: int,
        metric_name: str,
        df_kpi_map: pd.DataFrame,
        df_metric: pd.DataFrame,
        metadata: dict,
        enforce_existing_aggregations: bool,
    ):
        super().__init__(
            "prometheus",
            metric_index,
            metric_name,
            df_kpi_map,
            df_metric,
            enforce_existing_aggregations,
        )
        self.metadata = metadata

    def aggregate_kpis(self):
        # drop useless columns
        cols_to_drop = self.select_columns_to_drop()
        self.df_kpi_map = self.df_kpi_map.drop(cols_to_drop, axis=1)

        aggregation = self.aggregations[
            (self.aggregations["name"] == self.metric_name)
            & (self.aggregations["index"] == self.metric_index)
        ]

        if not aggregation.empty:
            # use an existing aggregation record
            return self.apply_existing_aggregation(aggregation)

        if self.enforce_existing_aggregations:
            msg = f"Missing aggregation record for metric {self.metric_index} {self.metric_name}!"
            logging.error(msg)
            raise ValueError(msg)

        if self.metric_name.startswith("prometheus.googleapis.com/kube_deployment"):
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                ["deployment"],
                [],
            )
            return self.df_metric.rename(columns=self.gen_map_columns("deployment"))
        elif self.metric_name.startswith(
            "prometheus.googleapis.com/kube_horizontalpodautoscaler"
        ):
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                [],
                [],
            )
            return self.df_metric.rename(columns=self.gen_map_columns())
        elif self.metric_name.startswith(
            "prometheus.googleapis.com/kube_pod_container_status"
        ):
            self.df_kpi_map.drop(["pod"], axis=1, inplace=True)
            group_columns = ["container"]
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                group_columns,
                ["sum"],
            )
            return self.aggregate_with_groups(group_columns, ["sum"])
        elif self.metric_name.startswith("prometheus.googleapis.com/kube_pod_status"):
            pattern_pod_names = "|".join(self.metadata["services"])
            self.df_kpi_map["service"] = self.df_kpi_map["pod"].str.extract(
                f"({pattern_pod_names})"
            )
            self.df_kpi_map = self.df_kpi_map.drop(["pod"], axis=1)
            group_columns = list(
                self.df_kpi_map.drop(columns=[AggregateHandler.COL_KPI_INDEX]).columns
            )
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                group_columns,
                ["sum"],
            )
            return self.aggregate_with_groups(group_columns, ["sum"])

        if len(self.df_kpi_map.columns) == 1 and len(self.df_kpi_map != 1):
            # aggregate KPIs without grouping any fields in KPI map
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                [],
                ["min", "max", "mean", "median", "first_quartile", "third_quartile"],
            )
            return self.apply_aggregation(self.df_metric)
        elif len(self.df_kpi_map == 1):
            # keep values
            column = self.df_metric.columns[0]
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                [],
                [],
            )
            return self.df_metric.rename(columns={column: "kpi-value"})
        else:
            group_columns = list(
                self.df_kpi_map.drop(columns=[AggregateHandler.COL_KPI_INDEX]).columns
            )
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                group_columns,
                ["min", "max", "mean", "median", "first_quartile", "third_quartile"],
            )
            return self.aggregate_with_groups(group_columns)

    def select_columns_to_drop(self) -> set:
        """
        Select columns with the same value and predefined useless columns
        but keep the column of KPI index.
        """
        df_kpi_map_nunique = self.df_kpi_map.drop(
            columns=[AggregateHandler.COL_KPI_INDEX]
        ).nunique()
        cols_to_drop = (
            set(df_kpi_map_nunique[df_kpi_map_nunique == 1].index)
            | PrometheusAggHandler.USELESS_COLUMNS
        )
        cols_to_drop = cols_to_drop.intersection(set(self.df_kpi_map.columns))
        # keep pod_phase if exists
        cols_to_drop.discard("pod_phase")
        cols_to_drop.discard("container")
        return cols_to_drop
