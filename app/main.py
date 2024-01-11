import os
from app.agg.strategy import Strategy
from app.gcloud_aggregator import GCloudAggregator
from app.gcloud_metrics import GCloudMetrics
from app.gcloud_separator import GCloudSeparator
import logging
from multiprocessing import Pool
import pandas as pd

from app.locust_aggregator import LocustAggregator


def aggregate_metrics(fname_exp_yaml: str, filename_metadata_yaml: str, exp_index: int):
    gcloud_aggregator = GCloudAggregator(
        fname_exp_yaml,
        filename_metadata_yaml,
        exp_index,
        strategy=Strategy.CONSIDER_POD_PHASES,
        enforce_existing_aggregations=False,
        for_normal_dataset=True,
    )
    gcloud_aggregator.aggregate_all_metrics()
    gcloud_aggregator.merge_all_metrics()


def separate_metrics(fname_exp_yaml: str, exp_index: int):
    gcloud_separator = GCloudSeparator(
        fname_exp_yaml, exp_index, strategy=Strategy.CONSIDER_POD_PHASES
    )
    gcloud_separator.separate_kpis()


def merge_normal_experiments(
    fname_exp_yaml: str, ignore_timestamp: bool, separated_locust: bool
):
    """
    Merge DataFrames of normal experiments into one DataFrame.

    Parameters
    ----------
    fname_exp_yaml : str
        filename of the experiment YAML
    ignore_timestamp : bool
        set True to ignore timestamps, this flag works only if separated_locust=True
    separated_locust : bool
        set True to process locust statistics from a list of experiments,
        otherwise process locust statistics from path_experiments only
    """
    exp_yaml = GCloudMetrics.parse_experiment_yaml(fname_exp_yaml)
    normal_exps = []
    for experiment in exp_yaml["experiments"]:
        exp_name = experiment["name"]
        print(f"Reading {exp_name}")
        # read gcloud KPIs into a DataFrame
        path_experiment_csv = os.path.join(
            exp_yaml["path_experiments"], exp_name, exp_name + ".csv"
        )
        df_exp = pd.read_csv(path_experiment_csv)
        df_exp["timestamp"] = pd.to_datetime(df_exp["timestamp"])
        if separated_locust:
            # read locust KPIs from the specified experiment folder into a DataFrame
            df_locust = LocustAggregator.read_locust_kpi(
                exp_yaml["path_experiments"], exp_name
            )
            df_locust = LocustAggregator.aggregate_all_metrics(df_locust)
            # merge locust KPIs with gcloud KPIs
            df_exp = df_exp.set_index("timestamp").join(df_locust, how="inner")
            if ignore_timestamp:
                df_exp.reset_index(drop=True, inplace=True)
        normal_exps.append(df_exp)
    output_path = os.path.join(
        exp_yaml["path_experiments"], fname_exp_yaml.removesuffix(".yaml") + ".csv"
    )
    df_normal_exps = pd.concat(normal_exps)
    if not separated_locust:
        # read locust KPIs from path_experiments into a DataFrame
        df_locust = LocustAggregator.read_locust_kpi(exp_yaml["path_experiments"], None)
        df_locust = LocustAggregator.aggregate_all_metrics(df_locust)
        # merge locust KPIs with gcloud KPIs
        df_normal_exps = (
            df_normal_exps.set_index("timestamp")
            .join(df_locust, how="inner")
            .reset_index()
        )

    df_normal_exps.to_csv(output_path, index=False)


def merge_experiment_with_locust_stats(fname_exp_yaml: str):
    exp_yaml = GCloudMetrics.parse_experiment_yaml(fname_exp_yaml)
    experiment = exp_yaml["experiments"][0]
    exp_name = experiment["name"]
    print(f"Merging {exp_name}")
    # read gcloud KPIs into a DataFrame
    path_experiment_csv = os.path.join(exp_yaml["path_experiments"], exp_name + ".csv")
    df_exp = pd.read_csv(path_experiment_csv)
    df_exp["timestamp"] = pd.to_datetime(df_exp["timestamp"])
    df_locust = LocustAggregator.read_locust_kpi(exp_yaml["path_experiments"], None)
    df_locust = LocustAggregator.aggregate_all_metrics(df_locust)
    # merge locust KPIs with gcloud KPIs
    df_normal_exps = (
        df_exp.set_index("timestamp").join(df_locust, how="inner").reset_index()
    )
    df_normal_exps.to_csv(path_experiment_csv, index=False)


def separate_metrics_with_multiprocess():
    inputs = [
        ("memory-stress-ts-auth-service-111717.yaml", 0),
        ("memory-stress-ts-auth-service-112220.yaml", 0),
        ("network-delay-ts-auth-service-112515.yaml", 0),
    ]
    num_processes = min(len(inputs), 8)
    with Pool(processes=num_processes) as pool:
        pool.starmap(separate_metrics, inputs)
        pool.close()
        pool.join()


def aggregate_metrics_with_multiprocess():
    # inputs = [("normal-2weeks.yaml", "train_ticket.yaml", i) for i in range(14)]
    inputs = [
        ("cpu-stress-ts-auth-service-111415.yaml", "train_ticket.yaml", 0),
        ("memory-stress-ts-auth-service-111717.yaml", "train_ticket.yaml", 0),
        ("memory-stress-ts-auth-service-112220.yaml", "train_ticket.yaml", 0),
        ("network-delay-ts-auth-service-112515.yaml", "train_ticket.yaml", 0),
    ]
    num_processes = min(len(inputs), 8)
    with Pool(processes=num_processes) as pool:
        pool.starmap(aggregate_metrics, inputs)
        pool.close()
        pool.join()


def process_single_experiment(fname_exp_yaml: str):
    separate_metrics(fname_exp_yaml, 0)
    aggregate_metrics(fname_exp_yaml, "train_ticket.yaml", 0)


def main():
    # define logging format
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(message)s",
    )
    # process_single_experiment("cpu-stress-ts-auth-service-111415.yaml")

    # separate_metrics_with_multiprocess()
    # aggregate_metrics_with_multiprocess()
    # merge_normal_experiments("normal-2weeks.yaml", False, True)
    merge_experiment_with_locust_stats("cpu-stress-ts-auth-service-111415.yaml")


if __name__ == "__main__":
    main()
