"""
Feature engineering class
"""
import pandas as pd


class FeatureEngineering:
    def add_volume(self):
        pass

    def buy_sell_ratio(self):
        pass

    def add_ask_to_bid_size_ratio(self, df):
        """Ask size to bid size ratio"""
        df["ask_to_bid_size_ratio"] = (df["ask_size1"] + df["ask_size2"]) / (
            df["bid_size1"] + df["bid_size2"]
        )
        return df

    def add_second_to_first_ratio(self, df):
        """Ratios of second bids to first bids.
        
        Are there a lot of bids on the second row but not on the first row? Signals how much buying pressure is around the market price but not at it. 
        """
        df["buyside_second_ratio"] = (df["ask_size2"] * df["ask_price2"]) / (
            df["ask_size1"] * df["ask_price1"]
        )
        df["sellside_second_ratio"] = (df["bid_size2"] * df["bid_price2"]) / (
            df["bid_size1"] * df["bid_price1"]
        )
        df["second_row_tension_metric"] = (df["ask_size2"] * df["ask_price2"]) / (
            df["bid_size1"] * df["bid_price1"]
        )

        return df

    def group_to_feature(self, df):
        """Capture Features """
        feature_sum = [
            "seconds_in_bucket",
            "bid_price1",
            "ask_price1",
            "bid_price2",
            "ask_price2",
            "bid_size1",
            "ask_size1",
            "bid_size2",
            "ask_size2",
        ]
        index = ["mean", "std", "min", "25%", "50%", "75%", "max"]
        val1 = df[feature_sum].describe().loc[index].values.reshape(-1)
        #     val2 = np.sum(df[feature_sum]).values
        return val1


    

    def merge(self, curr_book, curr_trade):
        """ This return a merged dataframe of trades where the trades actually took place """
        merged_data = pd.merge(
            curr_book, curr_trade, on=["time_id", "seconds_in_bucket"]
        )
        # merged_data['stock_id'] = stock_id
        #     print(curr_book.shape,curr_trade.shape,len(merged_data))
        # if len(merged_data) == 0:
        #     merged_data = curr_trade.merge(curr_book, how="cross", suffixes=["", "_y"])
        #     merged_data["diff"] = abs(
        #         merged_data.seconds_in_bucket - merged_data.seconds_in_bucket_y
        #     )
        #     merged_data = pd.merge(
        #         merged_data.groupby(["time_id", "seconds_in_bucket"])["diff"]
        #         .min()
        #         .reset_index(),
        #         merged_data,
        #         how="left",
        #     )
        #     merged_data.drop(
        #         columns=["time_id_y", "seconds_in_bucket_y", "diff"], inplace=True
        #     )
        #     # merged_data['stock_id'] = stock_id

        # merged_data.dropna(inplace=True)
        # merged_data.reset_index(drop=True)
        return merged_data
