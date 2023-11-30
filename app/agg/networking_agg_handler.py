import ast
import logging
import os
import pandas as pd
from app.agg.aggregate_handler import AggregateHandler
from app.agg.strategy import Strategy
from app.gcloud_metrics import GCloudMetrics


class NetworkingAggHandler(AggregateHandler):
    USELESS_COLUMNS = {
        "instance_id",
        "instance_name",
        "node_name",
        "network_tier",
        "remote_continent",
        "remote_country",
        "remote_cluster",
        "remote_network",
        "remote_project_id",
        "remote_region",
        "remote_subnetwork",
        "remote_zone",
        "resource_type",
        "remote_workload",
        "remote_workload_type",
        "remote_namespace",
        "remote_location_type",
        "remote_city",
    }

    def __init__(
        self,
        metric_index: int,
        metric_name: str,
        df_kpi_map: pd.DataFrame,
        df_metric: pd.DataFrame,
        metadata: dict,
        strategy: Strategy,
        enforce_existing_aggregations: bool,
    ):
        super().__init__(
            "networking",
            metric_index,
            metric_name,
            df_kpi_map,
            df_metric,
            enforce_existing_aggregations,
        )
        self.metadata = metadata
        self.strategy = strategy

    def aggregate_kpis(self):
        # drop useless columns
        cols_to_drop = self.select_columns_to_drop()
        self.df_kpi_map = self.df_kpi_map.drop(cols_to_drop, axis=1)
        self.transform_kpi_map()

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

        if len(self.df_kpi_map.columns) == 1:
            # aggregate KPIs without grouping any fields in KPI map
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                [],
                ["min", "max", "mean", "median", "first_quartile", "third_quartile"],
            )
            return self.apply_aggregation(self.df_metric)
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
            | NetworkingAggHandler.USELESS_COLUMNS
        )
        cols_to_drop = cols_to_drop.intersection(set(self.df_kpi_map.columns))
        # keep pod_phase if exists
        cols_to_drop.discard("pod_phase")
        return cols_to_drop

    def transform_kpi_map(self):
        """Transform KPI map into a human interpretable form."""
        if self.metric_name.startswith("networking.googleapis.com/pod_flow"):
            pattern_pod_names = "|".join(self.metadata["services"])
            self.df_kpi_map["service"] = self.df_kpi_map["pod_name"].str.extract(
                f"({pattern_pod_names})"
            )
            self.df_kpi_map = self.df_kpi_map.drop(["pod_name"], axis=1)
