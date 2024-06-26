import logging
import os
import pandas as pd
from app.agg.aggregate_handler import AggregateHandler
import csv
import ast


class ComputeAggHandler(AggregateHandler):
    USELESS_COLUMNS = {
        "instance_id",
        "instance_name",
        "project_id",
        "resource_type",
        "zone",
        "mount_option",
    }
    IP_PROTOCOL = {"1": "icmp", "6": "tcp", "17": "udp"}

    def __init__(
        self,
        metric_index: int,
        metric_name: str,
        df_kpi_map: pd.DataFrame,
        df_metric: pd.DataFrame,
        enforce_existing_aggregations: bool,
    ):
        super().__init__(
            "compute",
            metric_index,
            metric_name,
            df_kpi_map,
            df_metric,
            enforce_existing_aggregations,
        )

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
            logging.warning(msg)
            return None

        if set(self.df_kpi_map.columns) == {"kpi_index"}:
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
            | ComputeAggHandler.USELESS_COLUMNS
        )
        if self.metric_name.startswith("compute.googleapis.com/instance/disk"):
            cols_to_drop.add("device_name")
        cols_to_drop = cols_to_drop.intersection(set(self.df_kpi_map.columns))
        return cols_to_drop

    def transform_kpi_map(self):
        """Transform KPI map into a human interpretable form."""
        if "ip_protocol" in self.df_kpi_map.columns:
            self.df_kpi_map["ip_protocol"] = self.df_kpi_map["ip_protocol"].apply(
                lambda v: ComputeAggHandler.IP_PROTOCOL[str(v)]
            )
