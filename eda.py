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

# this code will actuall generate the reports


def generate_reports(dfs: list):
    """Generates Pandas Profiling reports"""
    for df in dfs:
        profile = ProfileReport(df)
        profile.to_file(outputfile=f"{df.name}.html")


# this will take a short while
generate_reports(dfs=[book_train, trade_train, book_test, trade_test])
# %%
