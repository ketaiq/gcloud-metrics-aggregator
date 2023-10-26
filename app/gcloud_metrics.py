import jsonlines
import json
import pandas as pd
import os
import yaml
from datetime import datetime


class GCloudMetrics:
    PATH_EXPERIMENTS_YAML = "experiments"
    FNAME_METRIC_TYPE_MAP = "metric_type_map.csv"
    FNAME_METRIC_TYPE_PREFIX = "metric-type-"
    FNAME_KPI_PREFIX = "kpi-"
    FNAME_KPI_SUFFIX = ".csv"
    FNAME_KPI_MAP = "kpi_map.jsonl"
    FNAME_NODES_INFO = "nodes_info"
    FNAME_PODS_INFO = "pods_info"
    FNAME_MERGED_KPIS = "gcloud_combined"
    EXP_NAME_PREFIX = "normal-"

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

        self.df_metric_type_map = pd.read_csv(
            os.path.join(self.path_metrics, GCloudMetrics.FNAME_METRIC_TYPE_MAP)
        ).set_index("index")

    def get_metric_indices(self, use_examples: bool = False) -> list:
        if use_examples:
            return [1, 74, 92, 94, 152]
        metric_indices = [
            int(fname.removeprefix(GCloudMetrics.FNAME_METRIC_TYPE_PREFIX))
            for fname in os.listdir(self.path_metrics)
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

    def read_nodes_metadata(self, exp_name: str) -> pd.DataFrame:
        """
        Read metadata of unique nodes in one experiment.

        Parameters
        ----------
        exp_name : str
            the name of an experiment

        Returns
        -------
        DataFrame
            metadata of nodes in pandas DataFrame, including name and instance_id
        """
        path_nodes_info = os.path.join(
            self.path_experiments, exp_name, GCloudMetrics.FNAME_NODES_INFO
        )
        nodes_metadata = {"instance_name": [], "instance_id": []}
        for filename in os.listdir(path_nodes_info):
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

    def read_kpi_map(self, metric_index: int) -> pd.DataFrame:
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
        path_kpi_map = self.build_path_kpi_map(metric_index)
        with jsonlines.open(path_kpi_map) as reader:
            kpi_map_list = [obj for obj in reader]
        kpi_maps = [kpi_map["kpi"] for kpi_map in kpi_map_list]
        indices = [kpi_map["index"] for kpi_map in kpi_map_list]
        return pd.DataFrame(kpi_maps, index=indices).sort_index(axis=1)

    def read_kpi(self, metric_index: int, kpi_index: int) -> pd.DataFrame:
        """
        Read values of a KPI per minute given the index.

        Parameters
        ----------
        metric_index : int
            the index of a metric type
        kpi_index : int
            the index of a KPI

        Returns
        -------
        DataFrame
            values of a KPI in pandas DataFrame with timestamps in rounded minutes as the index
        """
        path_kpi = self.build_path_kpi(metric_index, kpi_index)
        df_kpi = pd.read_csv(path_kpi)
        df_kpi = df_kpi.set_index("timestamp").add_prefix(f"kpi-{kpi_index}-")
        return df_kpi

    def build_path_kpi_map(self, metric_index: int) -> str:
        return os.path.join(
            self.path_metrics,
            GCloudMetrics.FNAME_METRIC_TYPE_PREFIX + str(metric_index),
            GCloudMetrics.FNAME_KPI_MAP,
        )

    def build_path_kpi(self, metric_index: int, kpi_index: int) -> str:
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
            GCloudMetrics.FNAME_MERGED_KPIS,
        )
        if not os.path.exists(path_folder_merged_kpis):
            os.mkdir(path_folder_merged_kpis)
        return path_folder_merged_kpis
