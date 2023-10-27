from abc import ABC
import json
import os

import pandas as pd


class Aggregator(ABC):
    @staticmethod
    def read_df_kpi_map(metric_index: int, kpi_map_folder_path: str) -> pd.DataFrame:
        kpi_map_path_json = os.path.join(
            kpi_map_folder_path, f"metric-{metric_index}-kpi-map.json"
        )
        kpi_map_path_csv = os.path.join(
            kpi_map_folder_path, f"metric-{metric_index}-kpi-map.csv"
        )
        if os.path.exists(kpi_map_path_json):
            with open(kpi_map_path_json) as fp:
                kpi_map_list = json.load(fp)
            kpi_maps = [kpi_map["kpi"] for kpi_map in kpi_map_list]
            indices = [kpi_map["index"] for kpi_map in kpi_map_list]
            df_kpi_map = pd.DataFrame(kpi_maps, index=indices).sort_index(axis=1)
            # ignore metrics from the traffic generator
            if "instance_name" in df_kpi_map.columns:
                mask = df_kpi_map["instance_name"] == "train-ticket-traffic-generator"
                df_kpi_map = df_kpi_map[~mask]
            if "instance_id" in df_kpi_map.columns:
                mask = df_kpi_map["instance_id"] == "8591042310161934693"
                df_kpi_map = df_kpi_map[~mask]
            return df_kpi_map
        elif os.path.exists(kpi_map_path_csv):
            return (
                pd.read_csv(kpi_map_path_csv).set_index("Unnamed: 0").sort_index(axis=1)
            )

    @staticmethod
    def reduce_cumulative(series: pd.Series) -> pd.Series:
        series = series.sub(series.shift())
        series = series.mask(series < 0)
        return series

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
