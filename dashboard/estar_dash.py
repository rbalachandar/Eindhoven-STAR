import streamlit as st
import datetime
import pandas as pd

from plotly.subplots import make_subplots
from fbprophet import Prophet
from fbprophet.plot import plot_plotly
from plotly import graph_objs as go
import json

# App title
st.markdown('''
# Eindhoven STAR (Sound, Temperature, Air Quality, Rain) Environment Dashboard
---
''')

file = 'all_output.json'
with open(file) as train_file:
    dict_train = json.load(train_file)

def string_to_dict(dict_string):
    # Convert to proper json format
    dict_string = dict_string.replace("'", '"').replace('u"', '"')
    return json.loads(dict_string)

data = string_to_dict(dict_train)

ticker_data = pd.DataFrame.from_dict(data)
ticker_data['avg_temp'] = ticker_data['avg_temp']/10
ticker_data['avg_rain'] = ticker_data['avg_rain']/10

fig = make_subplots(rows=5, cols=1,shared_xaxes=True,vertical_spacing=0.005, row_heights=[5,5,5,5,5])

fig.add_trace(go.Scattergl(x=ticker_data['date'], y=ticker_data['avg_no2'], name="Average No2 (µg/m³)"),row=1, col=1)
fig.layout.update(xaxis1_showgrid=False)
fig.add_trace(go.Scattergl(x=ticker_data['date'], y=ticker_data['avg_pm10'], name="Average PM10 (µg/m³)"),row=2, col=1)
fig.layout.update(xaxis2_showgrid=False)
fig.add_trace(go.Scattergl(x=ticker_data['date'], y=ticker_data['avg_laeq'], name="Average Sound laeq (dB(A))"),row=3, col=1)
fig.layout.update(xaxis3_showgrid=False)
fig.add_trace(go.Scattergl(x=ticker_data['date'], y=ticker_data['avg_temp'], name="Average Temperature (degC)"),row=4, col=1)
fig.layout.update(xaxis4_showgrid=False)
fig.add_trace(go.Scattergl(x=ticker_data['date'], y=ticker_data['avg_rain'], name="Average Rain (mm)"),row=5, col=1)
fig.layout.update(xaxis5_showgrid=False, height=1000, width=1000)
st.plotly_chart(fig)

# Predicting environment parameters Using Facebook’s Prophet
st.subheader('Predicting Eindhoven Environment parameters Using Facebook’s Prophet')

n_days = st.slider('Days of prediction:', 1, 365, value=31)
columns = ticker_data.columns.tolist()
parameters = {'avg_no2': "Avg Air No2", 'avg_pm10': "Avg Air PM10", 'avg_laeq': "Avg Sound Level", 'avg_temp': "Avg Temperature", 'avg_rain': "Avg Rain"}
del columns[0]
for column in columns:
    df_train = ticker_data[['date',column]]
    print(df_train)
    df_train = df_train.rename(columns={"date": "ds", column: "y"})

    m = Prophet()
    m.fit(df_train)
    future = m.make_future_dataframe(periods=n_days)
    forecast = m.predict(future)
        
    st.write(f'{parameters[column]} Forecast plot for {n_days} days')
    fig1 = plot_plotly(m, forecast)
    st.plotly_chart(fig1)
