import json
import os
import re
import warnings

import jsonlines
import pandas as pd
import yaml
from app.aggregator import Aggregator
from app.gcloud_metric_kind import GCloudMetricKind


class GCloudAggregator:
    PATH_EXPERIMENTS_YAML = "experiments"
    PATH_METADATA_YAML = "metadata"

    def __init__(
        self,
        experiment_name: str,
        filename_metadata_yaml: str,
        path_metrics: str,
    ):
        self.experiment_name = experiment_name
        self.path_metrics = path_metrics
        self.metadata = GCloudAggregator.parse_metadata_yaml(filename_metadata_yaml)
        self.parent_path_metrics = os.path.dirname(path_metrics)
        self.merged_submetrics_path = os.path.join(
            self.parent_path_metrics, "gcloud_combined"
        )
        self.aggregated_metrics_path = os.path.join(
            self.parent_path_metrics, "gcloud_aggregated"
        )
        self.complete_time_series_path = os.path.join(
            self.parent_path_metrics, "gcloud_complete_time_series.csv"
        )
        self.df_target_metrics = pd.read_csv(
            os.path.join(path_metrics, "metric_type_map.csv")
        ).set_index("index")
        if not os.path.exists(self.merged_submetrics_path):
            os.mkdir(self.merged_submetrics_path)
        if not os.path.exists(self.aggregated_metrics_path):
            os.mkdir(self.aggregated_metrics_path)

    @staticmethod
    def parse_experiment_yaml(filename_exp_yaml: str):
        """
        Parse the experiment YAML file.
        """
        path_exp_yaml = os.path.join(
            GCloudAggregator.PATH_EXPERIMENTS_YAML, filename_exp_yaml
        )
        with open(path_exp_yaml) as file_exp_yaml:
            return yaml.safe_load(file_exp_yaml)

    @staticmethod
    def parse_metadata_yaml(filename_metadata_yaml: str):
        path_metadata_yaml = os.path.join(
            GCloudAggregator.PATH_METADATA_YAML, filename_metadata_yaml
        )
        with open(path_metadata_yaml) as file_metadata_yaml:
            return yaml.safe_load(file_metadata_yaml)

    def _merge_submetrics(self, metric_path: str, metric_index: int):
        """Merge all available KPIs in one metric to produce a dataframe."""
        # copy KPI map to destination path
        kpi_map_path = os.path.join(
            metric_path,
            "kpi_map.jsonl",
        )
        with jsonlines.open(kpi_map_path) as reader:
            kpi_map_list = [obj for obj in reader]
        with open(
            os.path.join(
                self.merged_submetrics_path, f"metric-{metric_index}-kpi-map.json"
            ),
            "w",
        ) as fp:
            json.dump(kpi_map_list, fp)
        # merge KPIs in the metric type
        kpi_list = []
        for kpi_map in kpi_map_list:
            kpi_index = kpi_map["index"]
            kpi_path = os.path.join(
                metric_path,
                f"kpi-{kpi_index}.csv",
            )
            df_kpi = pd.read_csv(kpi_path)
            # round timestamp to minute
            df_kpi["timestamp"] = pd.to_datetime(
                df_kpi["timestamp"], unit="s"
            ).dt.round("min")
            df_kpi = df_kpi.set_index("timestamp").add_prefix(f"kpi-{kpi_index}-")
            # aggregate duplicated minutes
            if len(df_kpi.index) != len(df_kpi.index.drop_duplicates()):
                df_kpi = df_kpi.groupby("timestamp").agg("mean")
            kpi_list.append(df_kpi)
        df_kpis = pd.concat(kpi_list, axis=1)
        df_kpis.to_csv(
            os.path.join(self.merged_submetrics_path, f"metric-{metric_index}.csv")
        )

    def merge_all_submetrics(self):
        print(f"merging {self.experiment_name} ...")
        metric_types_indices = [
            f.split("-")[-1]
            for f in os.listdir(self.path_metrics)
            if f.startswith("metric-type")
        ]
        for metric_index in metric_types_indices:
            print(f"merging metric {metric_index} ...")
            metric_path = os.path.join(self.path_metrics, f"metric-type-{metric_index}")
            self._merge_submetrics(metric_path, metric_index)

    def get_df_metric(self, metric_index: int) -> pd.DataFrame:
        metric_path = os.path.join(
            self.merged_submetrics_path, f"metric-{metric_index}.csv"
        )
        df_metric = pd.read_csv(metric_path)
        df_metric["timestamp"] = pd.to_datetime(df_metric["timestamp"])
        df_metric = df_metric.set_index("timestamp").sort_index()
        metric_kind = self.df_target_metrics.loc[metric_index]["kind"]
        if metric_kind == GCloudMetricKind.CUMULATIVE.value:
            df_metric = df_metric.apply(Aggregator.reduce_cumulative)
        return df_metric

    def aggregate_one_metric(
        self, metric_index: int, for_node_localization: bool = False
    ):
        """Aggregate all available KPIs in one metric to reduce dimensionality."""
        metric_name = self.df_target_metrics.loc[metric_index]["name"]
        df_kpi_map = Aggregator.read_df_kpi_map(
            metric_index, self.merged_submetrics_path
        )
        if "node_name" in df_kpi_map.columns:
            df_kpi_map["node_name"] = df_kpi_map["node_name"].str.slice(-4)
        if metric_name.startswith("kubernetes.io/autoscaler"):
            self.adapt_no_agg(metric_index, df_kpi_map)
        elif metric_name.startswith("kubernetes.io/container"):
            self.aggregate_with_container_name(metric_index, df_kpi_map)
        elif metric_name.startswith("kubernetes.io/pod") or metric_name.startswith(
            "networking.googleapis.com/pod_flow"
        ):
            self.aggregate_with_pod_service(metric_index, df_kpi_map)
        elif (
            metric_name.startswith("kubernetes.io/node")
            and not for_node_localization
            or metric_name.startswith("networking.googleapis.com/vpc_flow")
            or metric_name.startswith("compute.googleapis.com")
        ):
            self.aggregate_with_all_kpis(metric_index, df_kpi_map)
        elif metric_name.startswith("kubernetes.io/node") and for_node_localization:
            self.adapt_no_agg(metric_index, df_kpi_map)
        elif (
            metric_name.startswith("networking.googleapis.com/node_flow")
            and for_node_localization
        ):
            self.aggregate_with_selected_labels(metric_index, df_kpi_map[["node_name"]])
        elif (
            metric_name.startswith("networking.googleapis.com/node_flow")
            and not for_node_localization
            or metric_name.startswith("networking.googleapis.com/vm_flow")
        ):
            if "remote_network" in df_kpi_map.columns:
                self.aggregate_with_selected_labels(
                    metric_index, df_kpi_map[["remote_network"]]
                )
            else:
                self.aggregate_with_all_kpis(metric_index, df_kpi_map)
        else:
            print(f"Unsupported aggregation on {metric_name}")

    @staticmethod
    def is_distribution(cols: list) -> bool:
        for col in cols:
            if "count" in col or "mean" in col or "sum_of_squared_deviation" in col:
                return True
        return False

    def aggregate_with_all_kpis(self, metric_index: int, df_kpi_map: pd.DataFrame):
        """Aggregate KPIs with only different node names."""
        print(f"Aggregating metric {metric_index} with all KPIs ...")
        group_columns = ["project_id"]
        df_same = df_kpi_map[group_columns]
        df_same = (
            df_same.reset_index().groupby(group_columns).agg(Aggregator.index_list)
        )
        self.aggregate(metric_index, df_same)

    def aggregate_with_container_name(
        self, metric_index: int, df_kpi_map: pd.DataFrame
    ):
        """Aggregate KPIs with same container name."""
        print(f"Aggregating metric {metric_index} with same container name ...")
        group_columns = ["container_name"]
        df_kpi_map_unique = df_kpi_map[group_columns]
        df_kpi_map_unique = (
            df_kpi_map_unique.reset_index()
            .groupby(group_columns)
            .agg(Aggregator.index_list)
        )
        self.aggregate(metric_index, df_kpi_map_unique)

    def aggregate_with_pod_service(self, metric_index: int, df_kpi_map: pd.DataFrame):
        """Aggregate KPIs with same pod service extracted from the pod name."""
        print(f"Aggregating metric {metric_index} with same pod service ...")
        pattern_pod_names = "|".join(self.metadata["services"])
        df_kpi_map["pod_name"] = df_kpi_map["pod_name"].str.extract(
            f"({pattern_pod_names})"
        )
        group_columns = ["pod_name"]
        df_kpi_map = df_kpi_map[group_columns]
        df_kpi_map_unique = (
            df_kpi_map.reset_index().groupby(group_columns).agg(Aggregator.index_list)
        )
        self.aggregate(metric_index, df_kpi_map_unique)

    def aggregate_with_selected_labels(
        self, metric_index: int, df_kpi_map: pd.DataFrame
    ):
        """Aggregate KPIs with selected labels."""
        print(f"Aggregating metric {metric_index} with selected labels ...")
        group_columns = df_kpi_map.columns.to_list()
        df_kpi_map_unique = (
            df_kpi_map.reset_index().groupby(group_columns).agg(Aggregator.index_list)
        )
        self.aggregate(metric_index, df_kpi_map_unique)

    def adapt_no_agg(self, metric_index: int, df_kpi_map: pd.DataFrame):
        """Adapt metrics no need for aggregation."""
        print(f"Adapting metric {metric_index} without aggregation ...")
        # drop columns with the same value
        nunique = df_kpi_map.nunique()
        cols_to_drop = nunique[nunique == 1].index
        df_kpi_map = df_kpi_map.drop(cols_to_drop, axis=1)
        # reduce duplicates
        df_kpi_map = df_kpi_map.T.drop_duplicates().T
        if (
            "container_name" in df_kpi_map.columns
            and "controller_name" in df_kpi_map.columns
        ):
            df_kpi_map = df_kpi_map[["container_name"]]
        # adapt kpi name
        df_metric = self.get_df_metric(metric_index)
        for i in df_kpi_map.index:
            original_column_name = f"kpi-{i}-value"
            if original_column_name in df_metric.columns:
                new_col_name = ""
                for k, v in df_kpi_map.loc[i].to_dict().items():
                    new_col_name += str(k) + "-" + str(v) + "-"
                new_col_name = new_col_name[:-1]
                df_metric.rename(
                    columns={original_column_name: new_col_name},
                    inplace=True,
                )
        df_metric.to_csv(
            os.path.join(self.aggregated_metrics_path, f"metric-{metric_index}.csv")
        )

    @staticmethod
    def gen_df_metric_agg(df_metric_to_agg: pd.DataFrame) -> pd.DataFrame:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            df_metric_agg = df_metric_to_agg.agg(
                [
                    "min",
                    "max",
                    "mean",
                    "median",
                    "count",
                    Aggregator.first_quartile,
                    Aggregator.third_quartile,
                ],
                axis=1,
            )
            return df_metric_agg

    def aggregate(self, metric_index: int, df_kpi_map_unique: pd.DataFrame):
        df_metric = self.get_df_metric(metric_index)
        df_agg_list = []
        df_kpi_map_unique = df_kpi_map_unique.rename(
            columns={"index": "index_list"}
        ).reset_index()
        for i in df_kpi_map_unique.index:
            column_prefix = df_kpi_map_unique[
                df_kpi_map_unique.drop(columns="index_list").columns[0]
            ].loc[i]
            # select columns to merge
            indices_with_metrics = [
                int(re.search(r"kpi-([0-9]+)-.*", column)[1])
                for column in df_metric.columns.to_list()
            ]
            valid_indices = list(
                set(df_kpi_map_unique.loc[i]["index_list"]) & set(indices_with_metrics)
            )
            if GCloudAggregator.is_distribution(df_metric.columns):
                for suffix in ["count", "mean"]:
                    columns_to_merge = [f"kpi-{i}-{suffix}" for i in valid_indices]
                    df_metric_to_agg = df_metric[columns_to_merge]
                    df_metric_agg = GCloudAggregator.gen_df_metric_agg(
                        df_metric_to_agg
                    ).add_prefix(f"{column_prefix}-d{suffix}-")
                    df_agg_list.append(df_metric_agg)
            else:
                columns_to_merge = [f"kpi-{i}-value" for i in valid_indices]
                df_metric_to_agg = df_metric[columns_to_merge]
                df_metric_agg = GCloudAggregator.gen_df_metric_agg(
                    df_metric_to_agg
                ).add_prefix(f"{column_prefix}-")
                df_agg_list.append(df_metric_agg)
        df_complete_agg = pd.concat(df_agg_list, axis=1)
        if not df_complete_agg.empty:
            df_complete_agg.to_csv(
                os.path.join(self.aggregated_metrics_path, f"metric-{metric_index}.csv")
            )

    def aggregate_all_metrics(self):
        """Aggregate all available metrics to reduce dimensionality."""
        metric_indices = self.get_metric_indices()
        for metric_index in metric_indices:
            if os.path.exists(
                os.path.join(self.aggregated_metrics_path, f"metric-{metric_index}.csv")
            ):
                continue
            self.aggregate_one_metric(metric_index, False)

    def merge_metrics(self):
        """Merge all metrics into one dataframe."""
        df_all_list = []
        metric_indices = self.get_metric_indices()
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

    def get_metric_indices(self) -> list:
        metric_indices = [
            int(filename.removeprefix("metric-").removesuffix("-kpi-map.json"))
            for filename in os.listdir(self.merged_submetrics_path)
            if filename.endswith("kpi-map.json")
        ]
        metric_indices.sort()
        return metric_indices
