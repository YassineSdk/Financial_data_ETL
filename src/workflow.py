
from prefect import flow,task ,get_run_logger
from dotenv import load_dotenv
from prefect.cache_policies import NO_CACHE
import os
# funtions 
from generic_data import synthic_data
from data_ingestion import ingest_data,connect_db
from data_concatenate import concatinate_data
from data_aggregation import aggregate_data
from bot_updater import send_repport_error ,send_repport_success
import warnings
warnings.filterwarnings('ignore')


load_dotenv(dotenv_path='../.env')
key = os.getenv("bot_key")
chat_id = os.getenv("chat_id")
connection_str = os.getenv('connection_str') 

#### Synthetic data generation ####
@task(cache_policy=NO_CACHE)
def data_generation(city):
    batch_data = synthic_data(city)
    return batch_data


#### Database connection ##### 
@task(cache_policy=NO_CACHE)
def db_connection():
    engine = connect_db()
    return engine

#### Data ingestion to the neon database ####
@task(cache_policy=NO_CACHE)
def data_ingestion(batch_data,engine,table):
    ingest_data(batch_data,engine,table)

#### Data concatination ####
@task(cache_policy=NO_CACHE)
def data_concatination(engine):
    data_size_1,data_size_t = concatinate_data(engine)
    return data_size_1,data_size_t

#### Data aggregation #### 
@task(cache_policy=NO_CACHE)
def data_aggregation(engine):
    aggregate_data(engine)

#### repporting success pipeline ####
@task(cache_policy=NO_CACHE)
def send_repport(data_size_1,data_size_t):
    send_repport_success(data_size_1,data_size_t)

#### repporting failed pipeline ####
@task(cache_policy=NO_CACHE)
def error_repport(error):
    send_repport_error(error)



#### data generation Subflow ####

@flow 
def data_generation_subflow():
    batch_casa = data_generation("casa")
    batch_rabat = data_generation("rabat")
    batch_tanger = data_generation("tanger")
    return batch_casa , batch_rabat , batch_tanger

#### data ingestion Subflow ####
@flow
def data_ingestion_subflow(batch_casa, batch_rabat, batch_tanger,engine):
    data_ingestion(batch_casa,engine,"data_casa")
    data_ingestion(batch_rabat,engine,"data_rabat")
    data_ingestion(batch_tanger,engine,"data_tanger")


@flow
def data_pipeline():
    try :
        engine = db_connection()
        batch_casa , batch_rabat , batch_tanger = data_generation_subflow()
        data_ingestion_subflow(batch_casa, batch_rabat, batch_tanger,engine)
        data_size_1,data_size_t = data_concatination(engine)
        data_aggregation(engine)
        send_repport(data_size_1,data_size_t )
        print(' the pipeline ran successfully')
    except Exception as e :
        error_repport(e)
        raise


data_pipeline()


