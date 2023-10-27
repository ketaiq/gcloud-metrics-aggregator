import os
from app.gcloud_metrics import GCloudMetrics
import logging
import pandas as pd
from datetime import datetime


class GCloudSeparator(GCloudMetrics):
    def __init__(self, filename_exp_yaml: str):
        super().__init__(filename_exp_yaml)

    def separate_kpis_by_experiments(self):
        logging.info("Separating KPIs by experiments ...")
        for metric_index in self.get_metric_indices_from_raw_dataset():
            metric_name = self.df_metric_type_map.loc[metric_index, "name"]
            logging.info(f"Processing metric type {metric_name} ...")
            df_kpi_map = self.read_kpi_map(metric_index).reset_index(names="kpi_index")

            for experiment in self.experiments:
                logging.info(
                    "Processing experiment {name} ...".format(name=experiment["name"])
                )
                if self.metric_merged_kpis_exists(metric_index, experiment["name"]):
                    continue
                df_exp_kpi_map = self.filter_kpis_in_one_experiment(
                    metric_name, experiment["name"], df_kpi_map
                )
                if df_exp_kpi_map is None or df_exp_kpi_map.empty:
                    continue
                self.merge_kpis_in_one_experiment(
                    metric_index, experiment, df_exp_kpi_map
                )

    def merge_kpis_in_one_experiment(
        self, metric_index: int, experiment: dict, df_exp_kpi_map: pd.DataFrame
    ):
        start_dt = datetime.fromisoformat(experiment["start"]).timestamp()
        end_dt = datetime.fromisoformat(experiment["end"]).timestamp()
        kpi_list = []
        for kpi_index in df_exp_kpi_map["kpi_index"]:
            df_kpi = self.read_kpi(metric_index, kpi_index)
            df_kpi = df_kpi[(df_kpi.index >= start_dt) & (df_kpi.index <= end_dt)]
            kpi_list.append(df_kpi)
        df_kpis = pd.concat(kpi_list, axis=1)
        path_folder_merged_kpis = self.build_path_folder_merged_kpis(experiment["name"])
        # save both KPI values and KPI map
        df_kpis.to_csv(
            os.path.join(
                path_folder_merged_kpis,
                f"metric-{metric_index}.csv",
            )
        )
        df_exp_kpi_map.to_csv(
            os.path.join(path_folder_merged_kpis, f"metric-{metric_index}-kpi-map.csv"),
            index=False,
        )

    def metric_merged_kpis_exists(self, metric_index: int, exp_name: str) -> bool:
        path_folder_merged_kpis = self.build_path_folder_merged_kpis(exp_name)
        path_merged_kpis = os.path.join(
            path_folder_merged_kpis,
            f"metric-{metric_index}.csv",
        )
        path_merged_kpis_map = os.path.join(
            path_folder_merged_kpis, f"metric-{metric_index}-kpi-map.csv"
        )
        return os.path.exists(path_merged_kpis) and os.path.exists(path_merged_kpis_map)

    def filter_kpis_in_one_experiment(
        self, metric_name: str, exp_name: str, df_kpi_map: pd.DataFrame
    ) -> pd.DataFrame | None:
        """
        Filter KPIs in one experiment from the KPI map.

        Returns
        -------
        DataFrame | None
            the filtered KPI map which only includes KPI indices from one experiment
        """
        if metric_name.startswith("compute.googleapis.com"):
            df_exp_kpi_map = self._filter_kpis_from_compute(exp_name, df_kpi_map)
        elif metric_name.startswith("networking.googleapis.com"):
            df_exp_kpi_map = self._filter_kpis_from_networking(exp_name, df_kpi_map)
        elif metric_name.startswith("prometheus.googleapis.com"):
            exp_id = exp_name.removeprefix(self.EXP_NAME_PREFIX)
            df_exp_kpi_map = df_kpi_map[df_kpi_map["cluster"].str.contains(exp_id)]
        elif metric_name.startswith("logging.googleapis.com"):
            df_exp_kpi_map = self._filter_kpis_from_logging(exp_name, df_kpi_map)
        elif metric_name.startswith("kubernetes.io"):
            exp_id = exp_name.removeprefix(self.EXP_NAME_PREFIX)
            df_exp_kpi_map = df_kpi_map[df_kpi_map["cluster_name"].str.contains(exp_id)]
        else:
            logging.error(f"Metric {metric_name} is not supported!")
            return None
        return df_exp_kpi_map

    def _filter_kpis_from_compute(
        self, exp_name: str, df_kpi_map: pd.DataFrame
    ) -> pd.DataFrame:
        df_node_metadata = self.read_nodes_metadata(exp_name)
        if (
            "instance_name" in df_kpi_map.columns
            and "instance_id" in df_kpi_map.columns
        ):
            filter_fields = ["instance_name", "instance_id"]
            return (
                df_kpi_map.set_index(filter_fields)
                .join(
                    df_node_metadata.set_index(filter_fields),
                    how="inner",
                )
                .reset_index()
            )
        elif "instance_id" in df_kpi_map.columns:
            filter_fields = ["instance_id"]
            return (
                df_kpi_map.set_index(filter_fields)
                .join(
                    df_node_metadata.set_index(filter_fields),
                    how="inner",
                )
                .reset_index()
            )
        elif "instance_group_name" in df_kpi_map.columns:
            grp_id = (
                df_node_metadata["instance_name"]
                .str.extract("gke-train-ticket-cluster-default-pool-([a-z0-9]+)")
                .iloc[0, 0]
            )
            grp_name = f"gke-train-ticket-cluster-default-pool-{grp_id}-grp"
            return df_kpi_map[df_kpi_map["instance_group_name"] == grp_name]
        else:
            return None

    def _filter_kpis_from_networking(
        self, exp_name: str, df_kpi_map: pd.DataFrame
    ) -> pd.DataFrame:
        if "cluster_name" in df_kpi_map.columns:
            exp_id = exp_name.removeprefix(self.EXP_NAME_PREFIX)
            return df_kpi_map[df_kpi_map["cluster_name"].str.contains(exp_id)]
        elif "instance_id" in df_kpi_map.columns:
            df_node_metadata = self.read_nodes_metadata(exp_name)
            return (
                df_kpi_map.set_index("instance_id")
                .join(
                    df_node_metadata.set_index("instance_id"),
                    how="inner",
                )
                .reset_index()
            )
        else:
            return None

    def _filter_kpis_from_logging(
        self, exp_name: str, df_kpi_map: pd.DataFrame
    ) -> pd.DataFrame:
        df_node_metadata = self.read_nodes_metadata(exp_name)
        df_instance_id = (
            df_kpi_map[~df_kpi_map["instance_id"].isna()]
            .set_index("instance_id")
            .join(
                df_node_metadata.set_index("instance_id"),
                how="inner",
            )
            .dropna(axis=1)
            .reset_index()
        )
        exp_id = exp_name.removeprefix(self.EXP_NAME_PREFIX)
        df_cluster_name = df_kpi_map[
            ~df_kpi_map["cluster_name"].isna()
            & df_kpi_map["cluster_name"].str.contains(exp_id)
        ].dropna(axis=1)
        return pd.concat([df_instance_id, df_cluster_name])
