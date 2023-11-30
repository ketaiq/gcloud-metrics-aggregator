import os

import pandas as pd
from app.gcloud_metrics import GCloudMetrics


def find_constant_metrics(fname_exp_yaml: str, num_days: int = 14):
    """Find constant metrics in the normal dataset."""
    exp_yaml = GCloudMetrics.parse_experiment_yaml(fname_exp_yaml)
    path_metric_type_map = os.path.join(
        exp_yaml["path_experiments"],
        exp_yaml["experiments"][0]["name"],
        GCloudMetrics.FDNAME_ORIGINAL_KPIS,
        GCloudMetrics.FNAME_METRIC_TYPE_MAP,
    )
    df_metric_type_map = pd.read_csv(path_metric_type_map).set_index("index")
    constant_metrics = {"index": [], "name": [], "experiment": []}
    for exp in exp_yaml["experiments"]:
        path_combined_metrics = os.path.join(
            exp_yaml["path_experiments"], exp["name"], GCloudMetrics.FDNAME_MERGED_KPIS
        )
        metric_indices = [
            int(filename.removeprefix("metric-").removesuffix("-kpi-map.csv"))
            for filename in os.listdir(path_combined_metrics)
            if filename.endswith("kpi-map.csv")
        ]
        metric_indices.sort()
        for metric_index in metric_indices:
            metric_name = df_metric_type_map.loc[metric_index]["name"]
            df_metric = read_combined_kpis(path_combined_metrics, metric_index)
            if df_metric.std().std() == 0:
                constant_metrics["index"].append(metric_index)
                constant_metrics["name"].append(metric_name)
                constant_metrics["experiment"].append(exp["name"])
    df_constant_metrics = pd.DataFrame(constant_metrics)
    df_count = df_constant_metrics.groupby(["index", "name"]).count()
    df_constant_metrics = (
        df_count[df_count["experiment"] == num_days]
        .reset_index()[["index", "name"]]
        .set_index("index")
    )
    df_reserved_metrics = pd.read_csv(
        os.path.join("aggregations", "reserved_metrics.csv")
    ).set_index("index")
    df_constant_metrics = df_constant_metrics.drop(df_reserved_metrics.index)
    df_constant_metrics.to_csv(os.path.join("aggregations", "constant_metrics.csv"))


def read_combined_kpis(path_combined_metrics: str, metric_index: int):
    metric_path = os.path.join(path_combined_metrics, f"metric-{metric_index}.csv")
    df_metric = pd.read_csv(metric_path)
    df_metric["timestamp"] = pd.to_datetime(df_metric["timestamp"], format="ISO8601")
    return df_metric.set_index("timestamp").sort_index()


def main():
    find_constant_metrics("normal-2weeks.yaml")


if __name__ == "__main__":
    main()
