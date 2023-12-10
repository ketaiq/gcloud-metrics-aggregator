# gcloud-metrics-aggregator
GCloud Metrics Aggregator is to aggregate and merge metrics collected from Google Cloud Monitoring.

## Quick Start

### Prerequisite
- Conda
```sh
conda create -n train-ticket -c conda-forge python3.10 pandas pyyaml jsonlines
```

```sh
# analyze dataset and find constant metrics from the normal dataset
python -m app.data_analyzer
```