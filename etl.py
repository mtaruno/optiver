#%%
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
import random
import glob
import gc
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")
import numpy as np # linear algebra

train = pd.read_csv("data/train.csv")
train


# visualizing volatility
# input the number of most & least volatility stock-time records to visualize
num_to_visualize = 10

most_volatility = train.nlargest(num_to_visualize, 'target')
print(most_volatility)
least_volatility = train.nsmallest(num_to_visualize, 'target')
print(least_volatility)

# %%
class CFG:   
    seed=2021
    n_fold=5
    max_model=10
    max_runtime_secs=180 #10800

def seed_everything(seed=42):
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)

seed_everything(seed=CFG.seed)
# %%
import pyarrow.parquet as pq
# ETL
def load_data(path):
    temp = pq.ParquetDataset(path)
    book_example = temp.read()
    book_example = book_example.to_pandas()
    return book_example

book_train = load_data('data/book_train.parquet/stock_id=0')
trade_train = load_data('data/trade_train.parquet/stock_id=0')
# %%
book_train.head()
trade_train.head()

# %%
book_train
# %%
