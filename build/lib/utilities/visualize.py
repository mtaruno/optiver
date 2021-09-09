# Visualization 
import seaborn as sns
import matplotlib.pyplot as plt
sns.set(style="ticks", color_codes=True)
import plotly.offline as pyo
import plotly.graph_objs as go
from abc import ABC, abstractmethod

class Visualize:
    def __init__(self):
        self.main = {
        "pacific coast": "#5B84B1FF",
        "black": "#101820FF",
        "orange": "#F2AA4CFF",
        "coral": "#FC766A",
        "red": "#DC5757",
        "blue": "#4547CA",
        "teal": "#8AF3CC",
        "cyan": 'rgb(0, 200, 200)'
    }

    def bar(self, display_type = "pyo", x: list = None, y: list = None, color ="#FFD700", title: str = "Bar Plot") -> None:
        trace1 = go.Bar(x=x,y=y, marker=dict(color=color))
        data = [trace1]
        layout = go.Layout(title=title, barmode="stack",
                    xaxis = dict(tickangle = 90,
                                showticklabels = True,
                                type = "category",
                                dtick = 1))
        fig = go.Figure(data = data, layout = layout)

        if display_type == "fig": # doesn't work for now
            fig.show()
        elif display_type == "pyo":
            pyo.plot(fig)
        
        print("Bar displayed...")


    def onedim_distplot(self, data: list, title: str = "Distplot", group_label = "distplot", 
        color = None) -> None:

        if color is None: color = self.main['cyan']

        import plotly.figure_factory as ff

        hist_data = [data]
        group_labels = [group_label] # name of the dataset
        fig = ff.create_distplot(hist_data, 
        group_labels, colors = [color])
        fig.update_layout(title_text=title)

        pyo.plot(fig)

        print("One dim distplot displayed...")

    def display_table(self, df):
        fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns),
                    fill_color='paleturquoise',
                    align='left'),
        cells=dict(values=[df[i] for i in df.columns],
                fill_color='lavender',
                align='left'))
        ])
        pyo.plot(fig)


class FinancialViz(Visualize):
    def candle(data,period='5d',interval='5m'):
        #declare figure
        fig = go.Figure()

        #Candlestick
        fig.add_trace(go.Candlestick(x=data.index,
                        open=data['Open'],
                        high=data['High'],
                        low=data['Low'],
                        close=data['Close'], name = 'market data'))

        # Add titles
        fig.update_layout(
            title=f'{ticker} live share price evolution',
            yaxis_title='Stock Price (USD per Shares)')

        # X-Axes
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=15, label="15m", step="minute", stepmode="backward"),
                    dict(count=45, label="45m", step="minute", stepmode="backward"),
                    dict(count=1, label="HTD", step="hour", stepmode="todate"),
                    dict(count=3, label="3h", step="hour", stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangebreaks=[
                # NOTE: Below values are bound (not single values), ie. hide x to y
                dict(bounds=["sat", "mon"]),  # hide weekends, eg. hide sat to before mon
                dict(bounds=[16, 9.5], pattern="hour"),  # hide hours outside of 9.30am 4pm
                # dict(values=["2019-12-25", "2020-12-24"])  # hide holidays (Christmas and New Year's, etc)
            ]
        )
        fig.show()
        
        return data