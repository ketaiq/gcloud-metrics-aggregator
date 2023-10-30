import pandas as pd


class LocustAggregator:
    @staticmethod
    def aggregate_all_metrics(path_locust_csv: str) -> pd.DataFrame:
        df_locust = pd.read_csv(path_locust_csv)
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
        ).dt.round("min")
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
        return df_locust
