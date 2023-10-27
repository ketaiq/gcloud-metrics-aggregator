import logging
import os
import re
import warnings

import pandas as pd
import yaml
from app.agg.compute_agg_handler import ComputeAggHandler
from app.agg.kubernetes_agg_handler import KubernetesAggHandler
from app.agg.logging_agg_handler import LoggingAggHandler
from app.agg.networking_agg_handler import NetworkingAggHandler
from app.agg.prometheus_agg_handler import PrometheusAggHandler
from app.aggregator import Aggregator
from app.gcloud_metric_kind import GCloudMetricKind
from app.gcloud_metrics import GCloudMetrics
from app.agg.aggregate_handler import AggregateHandler


class GCloudAggregator(GCloudMetrics):
    def __init__(self, filename_exp_yaml: str, filename_metadata_yaml: str):
        super().__init__(filename_exp_yaml)
        self.metadata = GCloudAggregator.parse_metadata_yaml(filename_metadata_yaml)
        self.parent_path_metrics = os.path.dirname(self.path_metrics)
        self.aggregated_metrics_path = os.path.join(
            self.parent_path_metrics, GCloudMetrics.FDNAME_AGGREGATED_KPIS
        )
        self.complete_time_series_path = os.path.join(
            self.parent_path_metrics, f"{self.experiment_name}.csv"
        )
        if not os.path.exists(self.aggregated_metrics_path):
            os.mkdir(self.aggregated_metrics_path)

    @staticmethod
    def parse_metadata_yaml(filename_metadata_yaml: str):
        """Parse the YAML file of metadata."""
        path_metadata_yaml = os.path.join(
            GCloudMetrics.PATH_METADATA_YAML, filename_metadata_yaml
        )
        with open(path_metadata_yaml) as file_metadata_yaml:
            return yaml.safe_load(file_metadata_yaml)

    def read_combined_kpis(self, metric_index: int) -> pd.DataFrame:
        metric_path = os.path.join(self.path_metrics, f"metric-{metric_index}.csv")
        df_metric = pd.read_csv(metric_path)
        df_metric["timestamp"] = pd.to_datetime(
            df_metric["timestamp"], unit="s"
        ).dt.round("min")
        df_metric = df_metric.set_index("timestamp").sort_index()
        # aggregate duplicated minutes
        if len(df_metric.index) != len(df_metric.index.drop_duplicates()):
            df_metric = df_metric.groupby("timestamp").agg("mean")
        metric_kind = self.df_metric_type_map.loc[metric_index]["kind"]
        if metric_kind == GCloudMetricKind.CUMULATIVE.value:
            df_metric = df_metric.apply(Aggregator.reduce_cumulative)
        return df_metric

    def aggregate_one_metric(self, metric_index: int):
        """Aggregate all available KPIs in one metric to reduce dimensionality."""
        metric_name = self.df_metric_type_map.loc[metric_index]["name"]
        df_kpi_map = pd.read_csv(
            os.path.join(self.path_metrics, f"metric-{metric_index}-kpi-map.csv")
        )
        df_metric = self.read_combined_kpis(metric_index)
        logging.info(f"Aggregating metric {metric_index} {metric_name} ...")

        df_agg_metric = None
        if metric_name.startswith("compute.googleapis.com"):
            df_agg_metric = ComputeAggHandler(
                metric_index, metric_name, df_kpi_map, df_metric
            ).aggregate_kpis()
        elif metric_name.startswith("networking.googleapis.com"):
            df_agg_metric = NetworkingAggHandler(
                metric_index, metric_name, df_kpi_map, df_metric, self.metadata
            ).aggregate_kpis()
        elif metric_name.startswith("prometheus.googleapis.com"):
            df_agg_metric = PrometheusAggHandler(
                metric_index, metric_name, df_kpi_map, df_metric, self.metadata
            ).aggregate_kpis()
        elif metric_name.startswith("logging.googleapis.com"):
            df_agg_metric = LoggingAggHandler(
                metric_index, metric_name, df_kpi_map, df_metric
            ).aggregate_kpis()
        elif metric_name.startswith("kubernetes.io"):
            df_agg_metric = KubernetesAggHandler(
                metric_index, metric_name, df_kpi_map, df_metric, self.metadata
            ).aggregate_kpis()
        else:
            logging.error(f"Metric {metric_name} is not supported!")

        logging.info(f"KPIs after aggregation are {df_agg_metric.columns}")
        if df_agg_metric is not None and not df_agg_metric.empty:
            df_agg_metric.to_csv(
                os.path.join(self.aggregated_metrics_path, f"metric-{metric_index}.csv")
            )

    def aggregate_all_metrics(self):
        """Aggregate all available metrics to reduce dimensionality."""
        metric_indices = self.get_metric_indices_from_combined_dataset()
        for metric_index in metric_indices:
            if os.path.exists(
                os.path.join(self.aggregated_metrics_path, f"metric-{metric_index}.csv")
            ):
                continue
            self.aggregate_one_metric(metric_index)

    def merge_all_metrics(self):
        """Merge all metrics into one dataframe."""
        df_all_list = []
        metric_indices = self.get_metric_indices_from_aggregated_dataset()
        for metric_index in metric_indices:
            print(f"Processing metric {metric_index} ...")
            metric_path = os.path.join(
                self.aggregated_metrics_path, f"metric-{metric_index}.csv"
            )
            df_metric = pd.read_csv(metric_path).set_index("timestamp")
            df_all_list.append(df_metric.add_prefix(f"metric-{metric_index}-"))
        df_all = pd.concat(df_all_list, axis=1)
        num_cols = len(df_all.columns)
        num_rows = len(df_all)
        print(f"{num_rows} rows x {num_cols} columns")
        df_all.to_csv(self.complete_time_series_path)

    def get_metric_indices_from_combined_dataset(self) -> list:
        metric_indices = [
            int(filename.removeprefix("metric-").removesuffix("-kpi-map.csv"))
            for filename in os.listdir(self.path_metrics)
            if filename.endswith("kpi-map.csv")
        ]
        metric_indices.sort()
        return metric_indices

    def get_metric_indices_from_aggregated_dataset(self) -> list:
        metric_indices = [
            int(filename.removeprefix("metric-").removesuffix(".csv"))
            for filename in os.listdir(self.aggregated_metrics_path)
        ]
        metric_indices.sort()
        return metric_indices
