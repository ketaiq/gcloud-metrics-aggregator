import jsonlines
import json
import pandas as pd
import os
import yaml
from datetime import datetime
import logging
import sys


class GCloudMetrics:
    PATH_EXPERIMENTS_YAML = "experiments"
    PATH_METADATA_YAML = "metadata"
    FNAME_METRIC_TYPE_MAP = "metric_type_map.csv"
    FNAME_METRIC_TYPE_PREFIX = "metric-type-"
    FNAME_KPI_PREFIX = "kpi-"
    FNAME_KPI_SUFFIX = ".csv"
    FNAME_KPI_MAP = "kpi_map.jsonl"
    FNAME_NODES_INFO = "nodes_info"
    FNAME_PODS_INFO = "pods_info"
    FDNAME_ORIGINAL_KPIS = "gcloud_metrics"
    FDNAME_MERGED_KPIS = "gcloud_combined"
    FDNAME_AGGREGATED_KPIS = "gcloud_aggregated"

    def __init__(self, filename_exp_yaml: str):
        exp_yaml = GCloudMetrics.parse_experiment_yaml(filename_exp_yaml)
        self.path_metrics = (
            exp_yaml["path_metrics"] if "path_metrics" in exp_yaml else None
        )
        self.path_experiments = (
            exp_yaml["path_experiments"] if "path_experiments" in exp_yaml else None
        )
        self.experiments = (
            exp_yaml["experiments"] if "experiments" in exp_yaml else None
        )
        self.experiment_name = (
            exp_yaml["experiment_name"] if "experiment_name" in exp_yaml else None
        )
        if "path_metric_type_map" in exp_yaml:
            self.path_metric_type_map = exp_yaml["path_metric_type_map"]
        elif "path_metrics" in exp_yaml:
            self.path_metric_type_map = exp_yaml["path_metrics"]
        else:
            self.path_metric_type_map = None

    def read_metric_type_map(self, exp_name: str = None) -> pd.DataFrame:
        if self.path_metric_type_map is not None:
            return pd.read_csv(
                os.path.join(
                    self.path_metric_type_map, GCloudMetrics.FNAME_METRIC_TYPE_MAP
                )
            ).set_index("index")
        else:
            path_metric_type_map = os.path.join(
                self.build_path_experiment_metrics(exp_name),
                GCloudMetrics.FNAME_METRIC_TYPE_MAP,
            )
            return pd.read_csv(path_metric_type_map).set_index("index")

    def get_metric_indices_from_raw_dataset(
        self, exp_name: str, use_examples: bool = False
    ) -> list:
        if use_examples:
            return [1, 74, 92, 94, 152]
        elif self.path_metrics:
            metric_indices = [
                int(fname.removeprefix(GCloudMetrics.FNAME_METRIC_TYPE_PREFIX))
                for fname in os.listdir(self.path_metrics)
                if fname.startswith(GCloudMetrics.FNAME_METRIC_TYPE_PREFIX)
            ]
            metric_indices.sort()
            return metric_indices
        else:
            metric_indices = [
                int(fname.removeprefix(GCloudMetrics.FNAME_METRIC_TYPE_PREFIX))
                for fname in os.listdir(self.build_path_experiment_metrics(exp_name))
                if fname.startswith(GCloudMetrics.FNAME_METRIC_TYPE_PREFIX)
            ]
            metric_indices.sort()
            return metric_indices

    @staticmethod
    def parse_experiment_yaml(filename_exp_yaml: str):
        """
        Parse the experiment YAML file.
        """
        path_exp_yaml = os.path.join(
            GCloudMetrics.PATH_EXPERIMENTS_YAML, filename_exp_yaml
        )
        with open(path_exp_yaml) as file_exp_yaml:
            return yaml.safe_load(file_exp_yaml)

    def read_nodes_metadata(self, experiment: dict) -> pd.DataFrame:
        """
        Read metadata of unique nodes in one experiment.

        Parameters
        ----------
        experiment : dict
            the dict of an experiment

        Returns
        -------
        DataFrame
            metadata of nodes in pandas DataFrame, including name and instance_id
        """
        start_dt = datetime.fromisoformat(experiment["start"]).timestamp()
        end_dt = datetime.fromisoformat(experiment["end"]).timestamp()
        path_nodes_info = self.build_path_nodes_info(experiment["name"])
        nodes_metadata = {"instance_name": [], "instance_id": []}
        for filename in os.listdir(path_nodes_info):
            info_dt = int(filename.removesuffix(".json"))
            if info_dt < start_dt or info_dt > end_dt:
                continue
            with open(os.path.join(path_nodes_info, filename)) as file_nodes_info:
                nodes_info = json.load(file_nodes_info)
            for node in nodes_info["items"]:
                nodes_metadata["instance_name"].append(node["metadata"]["name"])
                nodes_metadata["instance_id"].append(
                    node["metadata"]["annotations"][
                        "container.googleapis.com/instance_id"
                    ]
                )
        return pd.DataFrame(nodes_metadata).drop_duplicates()

    def build_path_nodes_info(self, exp_name: str) -> str:
        path_1 = os.path.join(self.path_experiments, GCloudMetrics.FNAME_NODES_INFO)
        path_2 = os.path.join(
            self.path_experiments, exp_name, GCloudMetrics.FNAME_NODES_INFO
        )
        if os.path.exists(path_1):
            return path_1
        elif os.path.exists(path_2):
            return path_2

    def read_kpi_map(self, metric_index: int, exp_name: str) -> pd.DataFrame:
        """
        Read KPI map for a metric type.

        Parameters
        ----------
        metric_index : int
            the index of a metric type

        Returns
        -------
        DataFrame
            a KPI map for the metric type in pandas DataFrame
        """
        path_kpi_map = self.build_path_kpi_map(metric_index, exp_name)
        with jsonlines.open(path_kpi_map) as reader:
            kpi_map_list = [obj for obj in reader]
        kpi_maps = [kpi_map["kpi"] for kpi_map in kpi_map_list]
        indices = [kpi_map["index"] for kpi_map in kpi_map_list]
        return pd.DataFrame(kpi_maps, index=indices).sort_index(axis=1)

    def read_kpi(
        self, metric_index: int, kpi_index: int, exp_name: str
    ) -> pd.DataFrame:
        """
        Read values of a KPI per minute given the index.

        Parameters
        ----------
        metric_index : int
            the index of a metric type
        kpi_index : int
            the index of a KPI
        exp_name : str
            the name of an experiment

        Returns
        -------
        DataFrame
            values of a KPI in pandas DataFrame with timestamps in rounded minutes as the index
        """
        path_kpi = self.build_path_kpi(metric_index, kpi_index, exp_name)
        df_kpi = pd.read_csv(path_kpi)
        df_kpi = df_kpi.set_index("timestamp").add_prefix(f"kpi-{kpi_index}-")
        return df_kpi

    def build_path_experiment_metrics(self, exp_name: str) -> str:
        return os.path.join(
            self.path_experiments, exp_name, GCloudMetrics.FDNAME_ORIGINAL_KPIS
        )

    def build_path_kpi_map(self, metric_index: int, exp_name: str) -> str:
        if self.path_metrics is None:
            return os.path.join(
                self.build_path_experiment_metrics(exp_name),
                GCloudMetrics.FNAME_METRIC_TYPE_PREFIX + str(metric_index),
                GCloudMetrics.FNAME_KPI_MAP,
            )
        else:
            return os.path.join(
                self.path_metrics,
                GCloudMetrics.FNAME_METRIC_TYPE_PREFIX + str(metric_index),
                GCloudMetrics.FNAME_KPI_MAP,
            )

    def build_path_kpi(self, metric_index: int, kpi_index: int, exp_name: str) -> str:
        if self.path_metrics is None:
            return os.path.join(
                self.build_path_experiment_metrics(exp_name),
                GCloudMetrics.FNAME_METRIC_TYPE_PREFIX + str(metric_index),
                GCloudMetrics.FNAME_KPI_PREFIX
                + str(kpi_index)
                + GCloudMetrics.FNAME_KPI_SUFFIX,
            )
        else:
            return os.path.join(
                self.path_metrics,
                GCloudMetrics.FNAME_METRIC_TYPE_PREFIX + str(metric_index),
                GCloudMetrics.FNAME_KPI_PREFIX
                + str(kpi_index)
                + GCloudMetrics.FNAME_KPI_SUFFIX,
            )

    def build_path_folder_merged_kpis(self, exp_name: str) -> str:
        path_folder_merged_kpis = os.path.join(
            self.path_experiments,
            exp_name,
            GCloudMetrics.FDNAME_MERGED_KPIS,
        )
        if not os.path.exists(path_folder_merged_kpis):
            os.mkdir(path_folder_merged_kpis)
        return path_folder_merged_kpis
