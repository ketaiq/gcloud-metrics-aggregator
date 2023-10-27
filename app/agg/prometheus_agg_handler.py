import pandas as pd
from app.agg.aggregate_handler import AggregateHandler


class PrometheusAggHandler(AggregateHandler):
    USELESS_COLUMNS = {"uid"}

    def __init__(
        self,
        metric_index: int,
        metric_name: str,
        df_kpi_map: pd.DataFrame,
        df_metric: pd.DataFrame,
        metadata: dict,
    ):
        super().__init__(metric_index, metric_name, df_kpi_map, df_metric)
        self.metadata = metadata

    def aggregate_kpis(self):
        # drop useless columns
        cols_to_drop = self.select_columns_to_drop()
        self.df_kpi_map = self.df_kpi_map.drop(cols_to_drop, axis=1)

        if self.metric_name.startswith("prometheus.googleapis.com/kube_deployment"):
            return self.df_metric.rename(columns=self.gen_map_columns("deployment"))
        elif self.metric_name.startswith(
            "prometheus.googleapis.com/kube_horizontalpodautoscaler"
        ):
            return self.df_metric.rename(columns=self.gen_map_columns())
        elif self.metric_name.startswith(
            "prometheus.googleapis.com/kube_pod_container_status"
        ):
            self.df_kpi_map.drop(["pod"], axis=1, inplace=True)
            group_columns = list(
                self.df_kpi_map.drop(columns=[AggregateHandler.COL_KPI_INDEX]).columns
            )
            return self.aggregate_with_groups(group_columns, ["sum"])
        elif self.metric_name.startswith("prometheus.googleapis.com/kube_pod_status"):
            pattern_pod_names = "|".join(self.metadata["services"])
            self.df_kpi_map["service"] = self.df_kpi_map["pod"].str.extract(
                f"({pattern_pod_names})"
            )
            self.df_kpi_map = self.df_kpi_map.drop(["pod"], axis=1)
            group_columns = list(
                self.df_kpi_map.drop(columns=[AggregateHandler.COL_KPI_INDEX]).columns
            )
            return self.aggregate_with_groups(group_columns, ["sum"])

        if len(self.df_kpi_map.columns) == 1:
            # aggregate KPIs without grouping any fields in KPI map
            return self.apply_aggregation(self.df_metric)
        elif len(self.df_kpi_map == 1):
            # keep values
            return self.df_metric.rename(columns={"kpi-1-value": "value"})
        else:
            group_columns = list(
                self.df_kpi_map.drop(columns=[AggregateHandler.COL_KPI_INDEX]).columns
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
            | PrometheusAggHandler.USELESS_COLUMNS
        )
        cols_to_drop = cols_to_drop.intersection(set(self.df_kpi_map.columns))
        return cols_to_drop

    def gen_map_columns(self, target_column: str = None):
        self.df_kpi_map = self.df_kpi_map.set_index("kpi_index")
        map_columns = {}
        for kpi_index in self.df_kpi_map.index:
            original_column_name = f"kpi-{kpi_index}-value"
            if target_column is None:
                map_columns[original_column_name] = "-".join(
                    list(
                        (
                            name.upper() + "-" + str(value)
                            for name, value in self.df_kpi_map.loc[kpi_index]
                            .to_dict()
                            .items()
                        )
                    )
                )
            else:
                map_columns[original_column_name] = self.df_kpi_map.loc[
                    kpi_index, target_column
                ]
        return map_columns
