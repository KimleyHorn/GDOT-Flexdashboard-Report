#import feather
import pandas as pd
import boto3
import os
import re
import io
from pandas.tseries.offsets import Day
from datetime import datetime
from glob import glob
import random
import string
from retrying import retry

def random_string(length):
    x = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(length)]) 
    return x +  datetime.now().strftime('%H%M%S%f')

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

ath = boto3.client('athena')
s3 = boto3.client('s3')

def upload_parquet(Bucket, Key, Filename):
    #print(Key)
    feather_filename = Filename
    # df = feather.read_dataframe(feather_filename)
    df = pd.read_feather(feather_filename).drop(columns = ['Date'], errors = 'ignore')
    # parquet_filename = feather_filename.replace('.feather', '.parquet')
    df.to_parquet('s3://{b}/{k}'.format(b=Bucket, k=Key))

    # df.to_parquet(parquet_filename)

    # s3.upload_file(Filename=parquet_filename, 
    #                Bucket=Bucket, 
    #                Key=Key)
    # os.remove(parquet_filename)
    
    date_ = re.search('\d{4}-\d{2}-\d{2}', Key).group(0)
    table_name = re.search('mark/(.*?)/date', Key).groups()[0]
    #print(table_name)
    template_string = 'ALTER TABLE {t} add partition (date="{d}") location "s3://{b}/{p}/"'
    partition_query = template_string.format(t = table_name,
                                             d = date_, 
                                             b = Bucket,
                                             p = os.path.dirname(Key))
    print(partition_query)
    
    response = ath.start_query_execution(QueryString = partition_query, 
                                         QueryExecutionContext={'Database': 'gdot_spm'},
                                         ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})
    #print('Response HTTPStatusCode:', response['HTTPStatusCode'])

@retry(wait_random_min=1000, wait_random_max=2000, stop_max_attempt_number=10)
def get_keys_(bucket, prefix):
    objs = s3.list_objects(Bucket = bucket, Prefix = prefix)
    if 'Contents' in objs:
        return [contents['Key'] for contents in objs['Contents']]

def get_keys(bucket, table_name, start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq=Day())
    prefixes = ["mark/{t}/date={d}".format(t=table_name, d=date_.strftime('%Y-%m-%d')) for date_ in dates]
    
    keys = [get_keys_(bucket, prefix_) for prefix_ in prefixes]
    keys = [get_keys_(bucket, prefix_) for prefix_ in prefixes]
    keys = list(filter(lambda x: x, keys)) # drop None entries
    keys = [y for x in keys for y in x] # flatten list
    
    return keys

def read_parquet(bucket, table_name, start_date, end_date, signals_list = None):
    
    def download_and_read_parquet(key):
        objs = s3.list_objects(Bucket = bucket, Prefix = key)
        contents = objs['Contents']
        date_ = re.search('\d{4}-\d{2}-\d{2}', key).group(0)
        response = s3.get_object(Bucket = bucket, Key = contents[0]['Key'])
        
        with io.BytesIO() as f:
            f.write(response['Body'].read())
            df = pd.read_parquet(f).assign(Date = date_)

        return df

    start_key = 'mark/{t}/date={d}'.format(t=table_name, d=start_date)
    end_key = 'mark/{t}/date={d}'.format(t=table_name, d=end_date)
    
    #check = in_date_range(start_key, end_key)
    
    keys = get_keys(bucket, table_name, start_date, end_date)
    if len(keys) > 0:
        #df = pd.concat([download_and_read_parquet(key) for key in keys], sort = True)
        dfs = [pd.read_parquet('s3://gdot-spm/{}'.format(key)).assign(Date = re.search('\d{4}-\d{2}-\d{2}', key).group(0)) for key in keys]
        df = pd.concat(dfs, sort=True)
        
        feather_filename = '{t}_{d}_{r}.feather'.format(t=table_name, d=start_date, r=random_string(12))
        df.reset_index().drop(columns=['index']).to_feather(feather_filename)
            
        return feather_filename
    
    else:
        return None
    
    #pd.concat(dfs, sort = True).reset_index().drop(columns=['index']).to_feather(feather_filename)
    #
    #return feather_filename
    #
    #dfs = []
    #keys = get_keys(bucket, table_name, start_date, end_date)
    #print(keys)
    #for key_list in keys:
    #    for key in key_list:
    #        filename = os.path.basename(key)
    #        date_ = re.search('\d{4}-\d{2}-\d{2}', key).group(0)
    #        s3.download_file(Bucket=bucket,
    #                         Key=key,
    #                         Filename=filename)
    #        df = pd.read_parquet(filename).assign(Date = datetime.strptime(date_, '%Y-%m-%d'))
    #        if signals_list is not None:
    #            df = df[df.SignalID.isin(signals_list)]
    #        dfs.append(df)
    #feather_filename = table_name + '.feather'
    #pd.concat(dfs, sort = True).reset_index().drop(columns=['index']).to_feather(feather_filename)
    
    return feather_filename
    
def read_parquet_local(table_name, start_date, end_date, signals_list = None):
    
    def read_parquet(fn):
        filename = os.path.basename(fn)
        date_ = re.search('\d{4}-\d{2}-\d{2}', fn).group(0)
        df = pd.read_parquet(filename).assign(Date = date_)

        return df
        
    def in_date_range(start_filename, end_filename):
        return lambda x: x >= start_filename and x <= end_filename

    start_filename = '/home/rstudio/Code/GDOT/MARK/{t}/date={d}'.format(t=table_name, d=start_date)
    end_filename = '/home/rstudio/Code/GDOT/MARK/{t}/date={d}'.format(t=table_name, d=end_date)
    
    check = in_date_range(start_filename, end_filename)
    
    feather_filename = table_name + '.feather'
    fns = list(filter(check, list(glob('/home/rstudio/Code/GDOT/MARK/{t}/*/*'.format(t=table_name)))))
    df = pd.concat([read_parquet(fn) for fn in fns], sort = True)
    if signals_list is not None:
        df = df[df.SignalID.isin(signals_list)]
    df.reset_index().to_feather(feather_filename)

    return feather_filename

def read_parquet_file(bucket, key):

    if 'Contents' in s3.list_objects(Bucket = bucket, Prefix = key):
        

        #objs = s3.list_objects(Bucket = 'gdot-spm', Prefix = key)
        #contents = objs['Contents']
        date_ = re.search('\d{4}-\d{2}-\d{2}', key).group(0)
        
        df = (pd.read_parquet('s3://{b}/{k}'.format(b = bucket, k = key))
                .assign(Date = lambda x: pd.to_datetime(date_, format = '%Y-%m-%d'))
                .rename(columns = {'Timestamp': 'TimeStamp'}))

        #response = s3.get_object(Bucket = 'gdot-spm', Key = contents[0]['Key'])

        #with io.BytesIO() as f:
        #    f.write(response['Body'].read())
        #    df = (pd.read_parquet(f)
        #            .assign(Date = lambda x: pd.to_datetime(date_, format = '%Y-%m-%d'))
        #            .rename(columns = {'Timestamp': 'TimeStamp'}))
    else:
        df = pd.DataFrame()
        
    return df

def get_s3data_dask(bucket, prefix):
    
    df = dd.read_parquet('s3://{b}/{p}'.format(b=bucket, p=prefix))
    
    # Can't have None data to convert to R data frame
    if sum(df.isnull().any(1).compute()) > 0:
        for c in df.select_dtypes(include=['int8', 'int16', 'int32', 'int64', 'float']).columns:
            df[c] = df[c].fillna(-1)
        for c in df.select_dtypes(include='object').columns:
            df[c] = df[c].fillna('')
    
    return df.compute()

def query_athena(query, database, output_bucket):

    response = ath.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://{}'.format(output_bucket)
        }
    )
    print ('Started query.')
    # Wait for s3 object to be created
    polling.poll(
            lambda: 'Contents' in s3.list_objects(Bucket=output_bucket, 
                                                  Prefix=response['QueryExecutionId']),
            step=0.5,
            timeout=30)
    print ('Query complete.')
    key = '{}.csv'.format(response['QueryExecutionId'])
    time.sleep(1)
    s3.download_file(Bucket=output_bucket, Key=key, Filename=key)
    df = pd.read_csv(key)
    os.remove(key)

    print ('Results downloaded.')
    return df


if __name__ == '__main__':
    
    Bucket = 'gdot-spm'
    Key = 'mark/comm_uptime/date=2019-02-15/cu_2019-02-15.parquet'
    Filename = 'cu_2019-02-15.feather'
    
    upload_parquet(Bucket, Key, Filename)
    
    read_parquet('gdot-spm', 'comm_uptime', '2019-02-15', '2019-02-15')
    
    