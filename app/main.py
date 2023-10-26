from app.gcloud_aggregator import GCloudAggregator
from app.gcloud_separator import GCloudSeparator
import logging


def aggregate_metrics(fname_exp_yaml: str):
    experiment = GCloudAggregator.parse_experiment_yaml(fname_exp_yaml)
    logging.info(
        "Processing {exp_name} ...".format(exp_name=experiment["experiment_name"])
    )
    gcloud_aggregator = GCloudAggregator(
        experiment["experiment_name"], "train_ticket.yaml", experiment["path_metrics"]
    )
    gcloud_aggregator.merge_all_submetrics()
    gcloud_aggregator.aggregate_all_metrics()


def separate_metrics(fname_exp_yaml: str):
    gcloud_separator = GCloudSeparator(fname_exp_yaml)
    gcloud_separator.separate_kpis_by_experiments()


def main():
    # define logging format
    logging.basicConfig(
        filename="gcloud-metrics-aggregator.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(message)s",
    )
    # aggregate_metrics("normal-3days.yaml")
    separate_metrics("normal-12h-1-4.yaml")


if __name__ == "__main__":
    main()
