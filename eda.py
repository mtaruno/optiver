"""
EDA of the financial data
"""

#%%
import utilities.ingest as u

# ingest
etl = u.ETL()
book_train = etl.load_data(etl.paths["book_train"])
trade_train = etl.load_data(etl.paths["trade_train"])
# %%
trade_train
# %%
# creating a dashboard
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table

# Add an import for pandas_datareader and datetime
import pandas_datareader.data as web
from datetime import datetime

#%%
from utilities.visualize import Visualize

v = Visualize()
v.display_table(trade_train)
# %%
v.display_table(book_train)
# %%
from pandas_profiling import ProfileReport

prof = ProfileReport(trade_train)
prof.to_file(output_file="visualizations/trade_train.html")

# %%
prof = ProfileReport(book_train)
prof.to_file(output_file="visualizations/book_train.html")

# %%
