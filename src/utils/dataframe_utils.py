from pandas import DataFrame

class DataFrameUtils():
    @staticmethod
    def copy_index_to_column(df: DataFrame) -> DataFrame:
        if df.index is None:
            raise ValueError("DataFrame must have an index!")

        if df.columns.__contains__(df.index.name):
            return df

        df.insert(0, df.index.name, df.index)
        return df
