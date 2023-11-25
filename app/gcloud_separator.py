import os
from app.agg.strategy import Strategy
from app.gcloud_metrics import GCloudMetrics
import logging
import pandas as pd
from datetime import datetime


class GCloudSeparator(GCloudMetrics):
    def __init__(
        self, filename_exp_yaml: str, strategy: Strategy, only_pod_metrics: bool = False
    ):
        super().__init__(filename_exp_yaml)
        self.only_pod_metrics = only_pod_metrics
        self.strategy = strategy

    def separate_kpis_by_experiments(self):
        for i in range(len(self.experiments)):
            self.separate_kpis(i)

    def separate_kpis(self, exp_index: int):
        logging.info("Separating KPIs by experiments ...")
        experiment = self.experiments[exp_index]
        logging.info("Processing experiment {name} ...".format(name=experiment["name"]))
        df_metric_type_map = self.read_metric_type_map(experiment["name"])
        for metric_index in self.get_metric_indices_from_raw_dataset(
            experiment["name"], self.only_pod_metrics
        ):
            metric_name = df_metric_type_map.loc[metric_index, "name"]
            logging.info(f"Processing metric type {metric_name} ...")
            df_kpi_map = self.read_kpi_map(
                metric_index, experiment["name"]
            ).reset_index(names="kpi_index")

            # skip metrics that are already separated
            if not self.only_pod_metrics and self.metric_merged_kpis_exists(
                metric_index, experiment["name"]
            ):
                continue
            df_exp_kpi_map = self.filter_kpis_in_one_experiment(
                metric_name, experiment, df_kpi_map
            )
            if df_exp_kpi_map is None or df_exp_kpi_map.empty:
                continue
            self.merge_kpis_in_one_experiment(metric_index, experiment, df_exp_kpi_map)

    def merge_kpis_in_one_experiment(
        self, metric_index: int, experiment: dict, df_exp_kpi_map: pd.DataFrame
    ):
        start_ts = datetime.fromisoformat(experiment["start"]).timestamp()
        end_ts = datetime.fromisoformat(experiment["end"]).timestamp()
        df_pods_metadata = self.read_pods_metadata(experiment["name"],
            start_ts, end_ts
        )
        kpi_list = []
        new_kpi_map = []
        for i in df_exp_kpi_map.index:
            new_kpi_map_item = df_exp_kpi_map.loc[i].to_dict()
            new_kpi_map_item["kpi_index"] = len(new_kpi_map)
            kpi_index = df_exp_kpi_map.loc[i]["kpi_index"]
            df_kpi = self.read_kpi(
                metric_index, kpi_index, experiment["name"], start_ts, end_ts
            )

            if self.strategy != Strategy.IGNORE_POD_PHASES:
                pod_name = GCloudSeparator.get_pod_name_if_exists(df_exp_kpi_map, i)
                if pod_name is None:
                    new_kpi_map.append(new_kpi_map_item)
                    kpi_list.append(df_kpi.add_prefix(f'kpi-{new_kpi_map_item["kpi_index"]}-'))
                    continue
                # separate KPIs by pod phase
                series_pod_phase = df_pods_metadata[
                    df_pods_metadata["pod_name"] == pod_name
                ].set_index("timestamp")["pod_phase"]
                df_kpi_with_pod_phase = df_kpi.join(series_pod_phase)

                # use the next valid observation to fill the gap in the column pod_phase
                df_kpi_with_pod_phase["pod_phase"] = df_kpi_with_pod_phase[
                    "pod_phase"
                ].bfill()
                for phase in df_kpi_with_pod_phase["pod_phase"].unique():
                    df_kpi_phase = df_kpi_with_pod_phase[
                        df_kpi_with_pod_phase["pod_phase"] == phase
                    ].drop(columns=["pod_phase"]).add_prefix(f'kpi-{new_kpi_map_item["kpi_index"]}-')
                    if not df_kpi_phase.empty:
                        new_kpi_map_item["pod_phase"] = phase
                        new_kpi_map.append(new_kpi_map_item.copy())
                        kpi_list.append(df_kpi_phase)
                        new_kpi_map_item["kpi_index"] = len(new_kpi_map)
            else:
                new_kpi_map.append(new_kpi_map_item)
                kpi_list.append(df_kpi.add_prefix(f'kpi-{new_kpi_map_item["kpi_index"]}-'))
        df_kpis = pd.concat(kpi_list, axis=1).reset_index()
        df_kpis["timestamp"] = df_kpis["timestamp"].apply(datetime.timestamp).apply(int)

        path_folder_merged_kpis = self.build_path_folder_merged_kpis(experiment["name"])
        # save both KPI values and KPI map
        df_kpis.to_csv(
            os.path.join(
                path_folder_merged_kpis,
                f"metric-{metric_index}.csv",
            ),
            index=False,
        )
        pd.DataFrame(new_kpi_map).to_csv(
            os.path.join(path_folder_merged_kpis, f"metric-{metric_index}-kpi-map.csv"),
            index=False,
        )

    @staticmethod
    def get_pod_name_if_exists(df_exp_kpi_map: pd.DataFrame, index: int) -> str | None:
        if "pod_name" in df_exp_kpi_map.columns:
            return df_exp_kpi_map.loc[index]["pod_name"]
        elif "pod" in df_exp_kpi_map.columns:
            return df_exp_kpi_map.loc[index]["pod"]
        else:
            return None

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
        self, metric_name: str, experiment: dict, df_kpi_map: pd.DataFrame
    ) -> pd.DataFrame | None:
        """
        Filter KPIs in one experiment from the KPI map.

        Returns
        -------
        DataFrame | None
            the filtered KPI map which only includes KPI indices from one experiment
        """
        cluster_suffix = experiment["cluster_suffix"]
        if metric_name.startswith("compute.googleapis.com"):
            df_exp_kpi_map = self._filter_kpis_from_compute(experiment, df_kpi_map)
        elif metric_name.startswith("networking.googleapis.com"):
            df_exp_kpi_map = self._filter_kpis_from_networking(experiment, df_kpi_map)
        elif metric_name.startswith("prometheus.googleapis.com"):
            df_exp_kpi_map = df_kpi_map[
                df_kpi_map["cluster"].str.contains(cluster_suffix)
            ]
        elif metric_name.startswith("logging.googleapis.com"):
            df_exp_kpi_map = self._filter_kpis_from_logging(experiment, df_kpi_map)
        elif metric_name.startswith("kubernetes.io"):
            df_exp_kpi_map = df_kpi_map[
                df_kpi_map["cluster_name"].str.contains(cluster_suffix)
            ]
        else:
            logging.error(f"Metric {metric_name} is not supported!")
            return None
        return df_exp_kpi_map

    def _filter_kpis_from_compute(
        self, experiment: dict, df_kpi_map: pd.DataFrame
    ) -> pd.DataFrame:
        df_node_metadata = self.read_nodes_metadata(experiment)
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
        self, experiment: dict, df_kpi_map: pd.DataFrame
    ) -> pd.DataFrame:
        if "cluster_name" in df_kpi_map.columns:
            cluster_suffix = experiment["cluster_suffix"]
            return df_kpi_map[df_kpi_map["cluster_name"].str.contains(cluster_suffix)]
        elif "instance_id" in df_kpi_map.columns:
            df_node_metadata = self.read_nodes_metadata(experiment)
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
        self, experiment: dict, df_kpi_map: pd.DataFrame
    ) -> pd.DataFrame:
        df_node_metadata = self.read_nodes_metadata(experiment)
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
        cluster_suffix = experiment["cluster_suffix"]
        df_cluster_name = df_kpi_map[
            ~df_kpi_map["cluster_name"].isna()
            & df_kpi_map["cluster_name"].str.contains(cluster_suffix)
        ].dropna(axis=1)
        return pd.concat([df_instance_id, df_cluster_name])
