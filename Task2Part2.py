from datetime import datetime, timedelta
from textwrap import dedent
import time
import yfinance as yf
import pandas as pd
import numpy as np
import math 
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import confusion_matrix, accuracy_score

tickers = ['AAPL', 'GOOGL', 'FB', 'MSFT', 'AMZN']

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'meghana',
    'depends_on_past': True,
    'email': ['mmj2169@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


def data_ingestion(**kwargs):
    global tickers
    for ticker in tickers:
        end_date = kwargs['execution_date']
        df = yf.Ticker(ticker)
        stock_history = df.history(period='max', end=end_date.strftime("%Y-%m-%d"))
        print(stock_history)
        stock_history.to_pickle(f"./stock_data_{ticker}.pkl")

def preprocess(**kwargs):
    global tickers
    for ticker in tickers:
        stock_history = pd.read_pickle(f"./stock_data_{ticker}.pkl")
        stock_history.drop(columns=["Dividends","Stock Splits"],inplace=True)
        stock_history.reset_index(inplace=True)
        stock_history['Date'] = pd.to_datetime(stock_history.Date)
        stock_history.to_pickle(f"./stock_data_{ticker}.pkl")

def train_model(**kwargs):
    global tickers
    for i,ticker in enumerate(tickers):
        stock_history = pd.read_pickle(f"./stock_data_{ticker}.pkl")
        end_date = kwargs['execution_date'].strftime("%Y-%m-%d")
        #print(stock_history.columns)

        if stock_history[stock_history['Date'] == end_date].shape[0] == 0:
            pass
        else:
            try:
                dfr=pd.read_csv(f'./{ticker}_error.csv')
                stock_history = stock_history.tail(11)
            except:
                stock_history = stock_history

            y = stock_history['High']
            x = stock_history[['Open', 'Low', 'Close', 'Volume']]
            
            train_x = x[:-1]
            train_y = y[:-1]
            test_x = x[-1:]
            test_y = y[-1:]

            #Linear Regression Model
            regression = LinearRegression()
            regression.fit(train_x, train_y)
            predicted = regression.predict(test_x)

            try:
                dfr   = pd.read_csv(f'./{ticker}_error.csv')
                dfr2  = pd.DataFrame({'Date':stock_history.Date.tail(1).values,'Actual_Price':test_y, 'Predicted_Price':predicted})
                dfr2['error'] = (dfr2["Predicted_Price"] - dfr2["Actual_Price"] )/(dfr2["Actual_Price"])
                df3 = pd.concat([dfr,dfr2])
                df3.to_csv(f'./{ticker}_error.csv',index=False)

            except:
                dfr=pd.DataFrame({'Date':stock_history.Date.tail(1).values,'Actual_Price':test_y, 'Predicted_Price':predicted})
                dfr['error'] = (dfr["Predicted_Price"] - dfr["Actual_Price"] )/(dfr["Actual_Price"])
                dfr.to_csv(f'./{ticker}_error.csv',index=False)

def create_csv(**kwargs):
    combined = []
    global tickers
    for ticker in tickers:
        dfr=pd.read_csv(f'./{ticker}_error.csv')
        dfr['Company'] =  str(ticker)
        dfr["Schedule"] =  dfr["Date"].str[:10]+" 07:00:00 am"
        dfr = dfr[["Schedule","Company","Actual_Price","Predicted_Price","error"]]
        dfr.drop_duplicates(inplace=True)
        combined.append(dfr.tail(5))
    dfc = pd.concat(combined)
    dfc.to_csv('./error.csv',index=False)


with DAG(
    'hw4_stock_prediction',
    default_args=default_args,
    description='Homework 4 Part 2', 
    schedule_interval='0 7 * * *', 
    start_date=datetime(2021, 11, 12),
    end_date=datetime(2021, 11, 28),
    catchup=True,
    tags=['hw4'],
) as dag:

    
    task_ingest_data = PythonOperator(
        task_id='ingest_data',
        python_callable=data_ingestion,
        provide_context=True,
        dag=dag
    )

    task_preprocess_data = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess,
        provide_context=True,
        dag=dag
    )

    task_train_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        provide_context=True,
        dag=dag
    )

    task_csv = PythonOperator(
        task_id='csv',
        python_callable=create_csv,
        provide_context=True,
        dag=dag
    )

    # task dependencies 

    task_ingest_data >> task_preprocess_data >> task_train_model >> task_csv