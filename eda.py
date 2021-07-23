'''
Visualizing volatility
'''

#%%
import utils
# ingest
etl = utils.ETL()
book_train = etl.load_data(etl.paths['book_train'])
trade_train = etl.load_data(etl.paths['trade_train'])
# %%
trade_train
# %%
# creating a dashboard

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output, State
# Add an import for pandas_datareader and datetime
import pandas_datareader.data as web
from datetime import datetime

#%%
from visualize import Visualize

v = Visualize()
v.display_table(trade_train)
# %%
v.display_table(book_train)
# %%
