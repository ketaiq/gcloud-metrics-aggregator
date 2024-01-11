import os
import pandas as pd


class LocustAggregator:
    FNAME_KPI = "train-ticket_stats_history.csv"

    @staticmethod
    def read_locust_kpi(path_experiments: str, exp_name: str) -> pd.DataFrame:
        # read from path_experiments
        path_locust_kpi = os.path.join(path_experiments, LocustAggregator.FNAME_KPI)
        if os.path.exists(path_locust_kpi):
            return pd.read_csv(path_locust_kpi)
        # read from single experiment
        path_locust_kpi = os.path.join(
            path_experiments, exp_name, LocustAggregator.FNAME_KPI
        )
        if os.path.exists(path_locust_kpi):
            return pd.read_csv(path_locust_kpi)

    @staticmethod
    def aggregate_all_metrics(df_locust: pd.DataFrame) -> pd.DataFrame:
        df_locust = df_locust[df_locust["Name"] == "Aggregated"].drop(
            columns=[
                "Type",
                "Name",
                "Total Request Count",
                "Total Failure Count",
                "Total Min Response Time",
                "Total Max Response Time",
            ]
        )
        df_locust["Timestamp"] = pd.to_datetime(
            df_locust["Timestamp"], unit="s"
        ).dt.floor("T")
        df_locust = df_locust.groupby("Timestamp").agg(
            {
                "User Count": "max",
                "Requests/s": "mean",
                "Failures/s": "mean",
                "25%": "max",
                "50%": "max",
                "75%": "max",
                "80%": "max",
                "90%": "max",
                "95%": "max",
                "98%": "max",
                "99%": "max",
                "99.9%": "max",
                "99.99%": "max",
                "100%": "max",
                "Total Median Response Time": "median",
                "Total Average Response Time": "mean",
                "Total Average Content Size": "mean",
            }
        )
        df_locust.index.rename("timestamp", inplace=True)
        df_locust = df_locust.add_prefix("lm-")
        return df_locust
