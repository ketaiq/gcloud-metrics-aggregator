import logging
import pandas as pd
from app.agg.aggregate_handler import AggregateHandler


class KubernetesAggHandler(AggregateHandler):
    USELESS_COLUMNS = {"controller_name", "node_name", "volume_name"}

    def __init__(
        self,
        metric_index: int,
        metric_name: str,
        df_kpi_map: pd.DataFrame,
        df_metric: pd.DataFrame,
        metadata: dict,
        enforce_existing_aggregations: bool,
    ):
        super().__init__(
            "kubernetes",
            metric_index,
            metric_name,
            df_kpi_map,
            df_metric,
            enforce_existing_aggregations,
        )
        self.metadata = metadata

    def aggregate_kpis(self):
        # drop useless columns
        cols_to_drop = self.select_columns_to_drop()
        self.df_kpi_map = self.df_kpi_map.drop(cols_to_drop, axis=1)
        self.transform_kpi_map()

        aggregation = self.aggregations[
            (self.aggregations["name"] == self.metric_name)
            & (self.aggregations["index"] == self.metric_index)
        ]
        if not aggregation.empty:
            # use an existing aggregation record
            return self.apply_existing_aggregation(aggregation)

        if self.enforce_existing_aggregations:
            msg = f"Missing aggregation record for metric {self.metric_index} {self.metric_name}!"
            logging.warning(msg)
            return None

        if self.metric_name.startswith("kubernetes.io/autoscaler"):
            self.df_kpi_map = self.df_kpi_map.set_index("kpi_index")
            map_columns = {}
            for kpi_index in self.df_kpi_map.index:
                original_column_name = f"kpi-{kpi_index}-value"
                map_columns[original_column_name] = self.df_kpi_map.loc[
                    kpi_index, "container_name"
                ]
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                ["container_name"],
                [],
            )
            return self.df_metric.rename(
                columns=map_columns,
            )

        if len(self.df_kpi_map.columns) == 1:
            # aggregate KPIs without grouping any fields in KPI map
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                [],
                ["min", "max", "mean", "median", "first_quartile", "third_quartile"],
            )
            return self.apply_aggregation(self.df_metric)
        else:
            group_columns = list(
                self.df_kpi_map.drop(columns=[AggregateHandler.COL_KPI_INDEX]).columns
            )
            self.record_new_aggregation(
                self.metric_index,
                self.metric_name,
                group_columns,
                ["min", "max", "mean", "median", "first_quartile", "third_quartile"],
            )
            return self.aggregate_with_groups(group_columns)

    def select_columns_to_drop(self) -> set:
        """
        Select columns with the same value and predefined useless columns
        but keep the column of KPI index.
        """
        df_kpi_map_nunique = self.df_kpi_map.drop(
            columns=[AggregateHandler.COL_KPI_INDEX]
        ).nunique()
        cols_to_drop = (
            set(df_kpi_map_nunique[df_kpi_map_nunique == 1].index)
            | KubernetesAggHandler.USELESS_COLUMNS
        )
        if self.metric_name.startswith("kubernetes.io/container"):
            cols_to_drop.add("pod_name")
        elif self.metric_name == "kubernetes.io/node/logs/input_bytes":
            cols_to_drop.discard("type")
        cols_to_drop = cols_to_drop.intersection(set(self.df_kpi_map.columns))
        # keep pod_phase if exists
        cols_to_drop.discard("pod_phase")
        return cols_to_drop

    def transform_kpi_map(self):
        """Transform KPI map into a human interpretable form."""
        if self.metric_name.startswith("kubernetes.io/pod"):
            pattern_pod_names = "|".join(self.metadata["services"])
            self.df_kpi_map["service"] = self.df_kpi_map["pod_name"].str.extract(
                f"({pattern_pod_names})"
            )
            self.df_kpi_map = self.df_kpi_map.drop(["pod_name"], axis=1)
