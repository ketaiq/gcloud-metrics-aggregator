import pandas as pd
from app.agg.aggregate_handler import AggregateHandler


class LoggingAggHandler(AggregateHandler):
    USELESS_COLUMNS = {"instance_id", "log", "resource_type", "instance_name"}

    def __init__(
        self,
        metric_index: int,
        metric_name: str,
        df_kpi_map: pd.DataFrame,
        df_metric: pd.DataFrame,
    ):
        super().__init__(metric_index, metric_name, df_kpi_map, df_metric)

    def aggregate_kpis(self):
        # drop useless columns
        cols_to_drop = self.select_columns_to_drop()
        self.df_kpi_map = self.df_kpi_map.drop(cols_to_drop, axis=1)

        if len(self.df_kpi_map.columns) == 1:
            # aggregate KPIs without grouping any fields in KPI map
            return self.apply_aggregation(self.df_metric)
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
        cols_to_drop = set(df_kpi_map_nunique[df_kpi_map_nunique == 1].index) | LoggingAggHandler.USELESS_COLUMNS
        cols_to_drop = cols_to_drop.intersection(set(self.df_kpi_map.columns))
        return cols_to_drop
