import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import os
from glob import glob
from tqdm import tqdm


class ETL:
    def __init__(self, seed=1000, stock_id="*"):
        self.seed = seed
        self.stock_id = stock_id
        # still with stock_id = 0, expand later
        self.paths = {
            "book_train": glob(
                "/Users/mtaruno/Documents/DevZone/optiver/data/book_train.parquet/"
            ),
            "trade_train": glob(
                "/Users/mtaruno/Documents/DevZone/optiver/data/trade_train.parquet"
            ),
            "book_test": glob(
                "/Users/mtaruno/Documents/DevZone/optiver/data/trade_test.parquet"
            ),
            "trade_test": glob(
                "/Users/mtaruno/Documents/DevZone/optiver/data/trade_test.parquet"
            ),
            "train": "/Users/mtaruno/Documents/DevZone/optiver/data/train.csv",
            "test": glob("/Users/mtaruno/Documents/DevZone/optiver/data/test.csv"),
            "sample_submission": glob(
                "/Users/mtaruno/Documents/DevZone/optiver/data/sample_submission.csv"
            ),
        }

    def load_data(self, path):
        temp = pq.ParquetDataset(path)
        read_temp = temp.read()
        return read_temp.to_pandas()

    def path_to_data(self, path):
        """ This return a merged dataframe of trades where the trades actually took place """
        #     print(path)
        stock_id = path.split("/")[-1].split("=")[1]
        curr_book = pd.read_parquet(path)
        curr_trade = pd.read_parquet(path.replace("book", "trade"))
        merged_data = pd.merge(
            curr_book, curr_trade, on=["time_id", "seconds_in_bucket"]
        )
        merged_data["stock_id"] = stock_id
        #     print(curr_book.shape,curr_trade.shape,len(merged_data))
        if len(merged_data) == 0:
            merged_data = curr_trade.merge(curr_book, how="cross", suffixes=["", "_y"])
            merged_data["diff"] = abs(
                merged_data.seconds_in_bucket - merged_data.seconds_in_bucket_y
            )
            merged_data = pd.merge(
                merged_data.groupby(["time_id", "seconds_in_bucket"])["diff"]
                .min()
                .reset_index(),
                merged_data,
                how="left",
            )
            merged_data.drop(
                columns=["time_id_y", "seconds_in_bucket_y", "diff"], inplace=True
            )
            merged_data["stock_id"] = stock_id
        merged_data.dropna(inplace=True)
        merged_data.reset_index(drop=True)
        return merged_data

    def read_all_files(self, path):
        """ Reads All file in the sub Folder (path / *) and read all parquets (trade/book) and picks only the first occurence based on Stock + Time
            Returns a list of all dataframes use concat to join them back ."""
        demo_all = []
        for i in tqdm(glob(os.path.join(path, "*"))):
            demo_merged = self.path_to_data(i)
            demo = demo_merged.groupby(["stock_id", "time_id"]).first().reset_index()
            demo.stock_id = demo.stock_id.astype("int64")
            demo_all.append(demo)
        return demo_all

    def files_to_numbers(
        self, demo_all, vol_calculated, csv_path="../data/train.csv",
    ):
        """ Takes in a List of DataFrame and Merges them with a CSV File and then with preprocessed data that we have where we calculate the Volatility
            at end of 10 min or bucket mark """
        csv_file = pd.read_csv(csv_path)
        demo = pd.concat(demo_all).reset_index(drop=True)
        demo_vol = pd.merge(csv_file, demo, on=["stock_id", "time_id"])
        demo_vol_all_data = pd.merge(demo_vol, vol_calculated)
        return demo_vol_all_data

    def seed_everything(self, seed=42):
        random.seed(seed)
        os.environ["PYTHONHASHSEED"] = str(seed)
        np.random.seed(seed)

    def ingest_all_parquet(self):
        """ Ingests all raw parquet data: book and trade train and test sets"""
        self.seed_everything(seed=self.seed)
        # data = {key: self.load_data(path) for key, path in self.paths.items()}

        data = (
            self.load_data(path)
            for path in (
                self.paths["book_train"],
                self.paths["trade_train"],
                self.paths["book_test"],
                self.paths["trade_test"],
            )
        )

        return data

    def main(self):
        # book_train, trade_train, book_test, trade_test = self.ingest_all_parquet()
        demo_all = self.read_all_files(
            "/Users/mtaruno/Documents/DevZone/optiver/data/book_train.parquet"
        )
        past_data = pd.read_csv(
            "/Users/mtaruno/Documents/DevZone/optiver/data/train.csv"
        )
        data = self.files_to_numbers(demo_all, past_data, self.paths["train"])
        display(data)

    def read_train_test(self):
        train = pd.read_csv("/Users/mtaruno/Documents/DevZone/optiver/data/train.csv")
        test = pd.read_csv("/Users/mtaruno/Documents/DevZone/optiver/data/test.csv")
        # Create a key to merge with book and trade data
        train["row_id"] = (
            train["stock_id"].astype(str) + "-" + train["time_id"].astype(str)
        )
        test["row_id"] = (
            test["stock_id"].astype(str) + "-" + test["time_id"].astype(str)
        )
        print(f"Our training set has {train.shape[0]} rows")
        return train, test

