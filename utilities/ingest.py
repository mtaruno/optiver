import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class ETL: 
    def __init__(self):
        # still with stock_id = 0, expand later
        self.paths = {
            'book_train' : 'data/book_train.parquet/stock_id=0',
            'trade_train': 'data/trade_train.parquet/stock_id=0',
            'book_test': 'data/trade_test.parquet/stock_id=0',
            'trade_test': 'data/trade_test.parquet/stock_id=0' 
        }

    def load_data(self, path):
        temp = pq.ParquetDataset(path)
        read_temp = temp.read()
        return read_temp.to_pandas()


def seed_everything(seed=42):
        class CFG:   
            seed=2021
            n_fold=5
            max_model=10
            max_runtime_secs=180 #10800
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)

seed_everything(seed=CFG.seed)

    def main(self):
        pass

