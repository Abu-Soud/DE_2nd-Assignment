import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def C_name():
    return 'Jordan'
def Get_DF_i(Day):
        import pandas as pd
        DF_i=None
        try: 
            URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
            DF_day=pd.read_csv(URL_Day)
            #DF_day.to_csv('/opt/airflow/data/AllData.csv')
            DF_day.Country_Region=DF_day.Country_Region.replace({' United Kingdom':'UK','United Kingdom':'UK'})
            DF_day['Day']=Day
            cond=(DF_day.Country_Region==C_name())
            Selec_columns=['Day','Country_Region', 'Last_Update',
                  'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
                  'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
            DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
        except:
            print(f'{Day} is not available!')
            pass
        return DF_i

def Generate_data():
        import pandas as pd
        List_of_Days=[]
        import datetime
        for i in range(1,145):
                Previous_Date = datetime.datetime.today() - datetime.timedelta(days=145-i)
                if (Previous_Date.day >9):
                    if (Previous_Date.month >9):
                          List_of_Days.append(f'{Previous_Date.month}-{Previous_Date.day}-{Previous_Date.year}')
                    else:
                          List_of_Days.append(f'0{Previous_Date.month}-{Previous_Date.day}-{Previous_Date.year}')
                else:
                    if (Previous_Date.month >9):
                          List_of_Days.append(f'{Previous_Date.month}-0{Previous_Date.day}-{Previous_Date.year}')
                    else:
                          List_of_Days.append(f'0{Previous_Date.month}-0{Previous_Date.day}-{Previous_Date.year}')
            
        DF_all=[]
        for Day in List_of_Days:
            DF_all.append(Get_DF_i(Day))
            
        DF=pd.concat(DF_all).reset_index(drop=True)
        DF['Last_Update']=pd.to_datetime(DF.Last_Update, infer_datetime_format=True)  
        DF['Day']=pd.to_datetime(DF.Day, infer_datetime_format=True)  
        DF['Case_Fatality_Ratio']=DF['Case_Fatality_Ratio'].astype(float)
        DF.set_index('Day', inplace=True)
        DF.to_csv(f'/opt/airflow/data/DF_{C_name()}.csv')

def min_max_scaler():
        import pandas as pd
        import sklearn
        from sklearn.preprocessing import MinMaxScaler
        DF = pd.read_csv(f'/opt/airflow/data/DF_{C_name()}.csv', parse_dates=['Last_Update'])
        DF['Day']=DF.Day
        DF.set_index('Day', inplace=True)
        min_max_scaler = MinMaxScaler()
        DF_u=DF.copy()
        Select_Columns=['Confirmed','Deaths', 'Recovered', 'Active','Case_Fatality_Ratio']
        DF_u_2=DF_u[Select_Columns]
        DF_u_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_u_2[Select_Columns]),columns=Select_Columns)
        DF_u_3['Day']=DF_u.index
        DF_u_3.set_index('Day', inplace=True)
        DF_u_3.to_csv(f'/opt/airflow/data/DF_{C_name()}_Scaled.csv')
        DF_u_3.to_csv(f'/opt/airflow/output/{C_name()}_scoring_report.csv')

def plot_Data():
        import pandas as pd 
        import matplotlib.pyplot as plt
        DF_u_3 = pd.read_csv(f'/opt/airflow/data/DF_{C_name()}_Scaled.csv', parse_dates=['Day'])
        DF_u_3.set_index('Day', inplace=True) 
        Select_Columns=['Confirmed','Deaths', 'Recovered', 'Active','Case_Fatality_Ratio']
        DF_u_3[Select_Columns].plot(figsize=(30,20))
        plt.savefig(f'/opt/airflow/output/{C_name()}_scoring_report.png')
        
    

def Load_to_Postgres():
        import sqlalchemy
        from sqlalchemy import create_engine
        import pandas as pd

        DF_Not_Scaled = pd.read_csv(f'/opt/airflow/data/DF_{C_name()}.csv', parse_dates=['Day'])
        DF_Not_Scaled.set_index('Day', inplace=True)

        DF_Scaled = pd.read_csv(f'/opt/airflow/data/DF_{C_name()}_Scaled.csv', parse_dates=['Day'])
        DF_Scaled.set_index('Day', inplace=True)

        host="postgres"
        database="airflow"
        user="airflow"
        password="airflow"
        port='5432'
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

        DF_Scaled.to_sql(f'{C_name()}_scoring_report', engine,if_exists='replace',index=False)  
        DF_Not_Scaled.to_sql(f'{C_name()}_scoring_notscaled_report', engine,if_exists='replace',index=False) 
 
 
default_args = {
    'owner': 'Mohammad',
    'start_date': dt.datetime(2022, 5, 29),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}
 
with DAG(f'{C_name()}_COVID19_dag',
         default_args=default_args,
         schedule_interval=timedelta(days=1),  
         catchup=False,     
         ) as dag:
    
    Install_Req = BashOperator(task_id='Install_Req',bash_command='pip install sklearn matplotlib')   
    Extracting = PythonOperator(task_id=f'Extracting_{C_name()}_Data', python_callable=Generate_data)
    Scalling = PythonOperator(task_id=f'Scalling_{C_name()}_Data', python_callable=min_max_scaler)
    Plotting = PythonOperator(task_id=f'Plotting_{C_name()}_Data', python_callable=plot_Data)
    Loading_to_Postgres = PythonOperator(task_id=f'Loading_{C_name()}_Data_to_Postgres', python_callable=Load_to_Postgres)

 
 
 

Install_Req >> Extracting >> Scalling >> Plotting >> Loading_to_Postgres