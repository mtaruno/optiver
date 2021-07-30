import numpy as np 
import pandas as pd 
import os
from glob import glob
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tqdm import tqdm
from sklearn.metrics import *
from sklearn.model_selection import *
from sklearn.preprocessing import *
from sklearn.linear_model import LinearRegression
import lightgbm as lgb
import xgboost as xgb
from sklearn.tree import *
from sklearn.ensemble import *


train = pd.read_csv('../data/train.csv')
test  = pd.read_csv('../data/test.csv')
sample_sub = pd.read_csv('../data/sample_submission.csv')

display(train)
display(test)
display(sample_sub)


def add_two_numbers(a,b):
    """ Add two numbers and return the sum """
    return a + b

def calculate_correlation_coefficient(x: list,y:list):
    """ Calculate correlation coefficient between two variables """
        return np.corrcoef(x,y)[0,1]
