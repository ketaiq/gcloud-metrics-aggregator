import os
from app import GCLOUD_TARGET_METRICS_PATH

from app.gcloud_aggregator import GCloudAggregator


def main():
    experiment = GCloudAggregator.parse_experiment_yaml("normal-3days.yaml")
    print("Processing {exp_name} ...".format(exp_name=experiment["experiment_name"]))
    gcloud_aggregator = GCloudAggregator(
        experiment["experiment_name"], "train_ticket.yaml", experiment["path_metrics"]
    )
    gcloud_aggregator.merge_all_submetrics()
    gcloud_aggregator.aggregate_all_metrics()


if __name__ == "__main__":
    main()
