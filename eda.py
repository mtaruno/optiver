"""
EDA of the financial data
"""

#%%
import utilities.ingest as u
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from datetime import datetime
from utilities.visualize import Visualize

# ingest all tables
etl = u.ETL()
book_train, trade_train, book_test, trade_test = etl.ingest_all_parquet()

#%%
book_train.head()
trade_train.head()

#%%
# def visualize_table(df):
#     """Visualizes table using plotly"""
#     v = Visualize()
#     v.display_table(df)

# visualize_table(trade_train)
# %%
from pandas_profiling import ProfileReport

# this code will actually generate the reports

def generate_reports(dfs: dict):
    """Generates Pandas Profiling reports"""
    for name, df in dfs.items():
        print(name)
        display(df.head())
        # profile = ProfileReport(df)
        # profile.to_file(f"{name}.html")

# this will take a short while
generate_reports(dfs={"book_train": book_train, "trade_train": trade_train, "book_test": book_test, "trade_test": trade_test})
# %%
trade_train
# %%

df['buy_side_ratio'] = (df['ask_size2'] * df['ask_price2'] )/ (df['ask_size1'] * df['ask_price1'])
df['sell_side_ratio'] = (df['bid_size2'] * df['bid_price2'] )/ (df['bid_size1'] * df['bid_price1'])

# %%
book_train.head()
# %%
