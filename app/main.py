import os
from app.gcloud_aggregator import GCloudAggregator
from app.gcloud_metrics import GCloudMetrics
from app.gcloud_separator import GCloudSeparator
import logging
from multiprocessing import Pool
import pandas as pd

from app.locust_aggregator import LocustAggregator


def aggregate_metrics(fname_exp_yaml: str, filename_metadata_yaml: str):
    gcloud_aggregator = GCloudAggregator(fname_exp_yaml, filename_metadata_yaml)
    gcloud_aggregator.aggregate_all_metrics()
    gcloud_aggregator.merge_all_metrics(ignore_buffer=True)


def separate_metrics(fname_exp_yaml: str):
    gcloud_separator = GCloudSeparator(fname_exp_yaml)
    gcloud_separator.separate_kpis_by_experiments()


def merge_normal_experiments(fname_exp_yaml: str, ignore_timestamp: bool):
    exp_yaml = GCloudMetrics.parse_experiment_yaml(fname_exp_yaml)
    normal_exps = []
    for experiment in exp_yaml["experiments"]:
        exp_name = experiment["name"]
        print(f"Reading {exp_name}")
        path_experiment_csv = os.path.join(
            exp_yaml["path_experiments"], exp_name, exp_name + ".csv"
        )
        path_locust_csv = os.path.join(
            exp_yaml["path_experiments"], exp_name, "train-ticket_stats_history.csv"
        )
        df_exp = pd.read_csv(path_experiment_csv)
        df_exp["timestamp"] = pd.to_datetime(df_exp["timestamp"])
        df_locust = LocustAggregator.aggregate_all_metrics(path_locust_csv)
        df_exp = df_exp.set_index("timestamp").join(df_locust, how="inner")
        if ignore_timestamp:
            df_exp.reset_index(drop=True, inplace=True)
        normal_exps.append(df_exp)
    output_path = os.path.join(
        exp_yaml["path_experiments"], fname_exp_yaml.removesuffix(".yaml") + ".csv"
    )
    pd.concat(normal_exps).to_csv(output_path, index=False)


def main():
    # define logging format
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(message)s",
    )
    # separate_metrics("normal-12h-1-4.yaml")
    inputs = (
        ("normal-12h-1.yaml", "train_ticket.yaml"),
        ("normal-12h-2.yaml", "train_ticket.yaml"),
        ("normal-12h-3.yaml", "train_ticket.yaml"),
        ("normal-12h-4.yaml", "train_ticket.yaml"),
    )
    with Pool(processes=len(inputs)) as pool:
        pool.starmap(aggregate_metrics, inputs)
        pool.close()
        pool.join()

    merge_normal_experiments("normal-12h-1-4.yaml", True)


if __name__ == "__main__":
    main()
