import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import os


class ETL:
    def __init__(self, seed=1000):
        self.seed = seed
        # still with stock_id = 0, expand later
        self.paths = {
            "book_train": "data/book_train.parquet/",
            "trade_train": "data/trade_train.parquet/",
            "book_test": "data/trade_test.parquet/",
            "trade_test": "data/trade_test.parquet/",
        }

    def load_data(self, path):
        temp = pq.ParquetDataset(path)
        read_temp = temp.read()
        return read_temp.to_pandas()

    def seed_everything(self, seed=42):
        random.seed(seed)
        os.environ["PYTHONHASHSEED"] = str(seed)
        np.random.seed(seed)

    def ingest_all_parquet(self):
        """ Ingests all raw parquet data """
        self.seed_everything(seed=self.seed)
        data = {key: self.load_data(path) for key, path in self.paths.items()}

        return (
            data["book_train"],
            data["trade_train"],
            data["book_test"],
            data["trade_test"],
        )

    def load_all_train_data(self):
        book_train = self.load_data(self.paths["book_train"])
        trade_train = self.load_data(self.paths["trade_train"])

        return book_train, trade_train

    def main(self):
        book_train, trade_train, book_test, trade_test = self.raw_ingest()

