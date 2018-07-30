# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
from multiprocessing.dummy import Pool
import pandas as pd
import sqlalchemy as sq
import time
import os
import itertools
from spm_events import etl_main
import boto3
import yaml

s3 = boto3.client('s3')
ath = boto3.client('athena')


'''
    df:
        SignalID [int64]
        TimeStamp [datetime]
        EventCode [str or int64]
        EventParam [str or int64]
    
    det_config:
        SignalID [int64]
        IP [str]
        PrimaryName [str]
        SecondaryName [str]
        Detector [int64]
        Call Phase [int64]
'''

def etl2(s, date_):
    
    left = det_config[det_config.SignalID==s]
    right = bad_detectors[(bad_detectors.SignalID==s) & (bad_detectors.Date==date_)]
        
    det_config_good = (pd.merge(left, right, how = 'outer', indicator = True)
                         .loc[lambda x: x._merge=='left_only']
                         .drop(['Date','_merge'], axis=1))
    
    query = """SELECT * FROM Controller_Event_Log 
               WHERE SignalID = '{}'
               AND EventCode in (1,4,5,6,8,9,31,81,82) 
               AND (Timestamp BETWEEN '{}' AND '{}');
               """
    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)
    
    
    t0 = time.time()
    print('{} | {} Starting...'.format(s, start_date))

    try:
        print('|{} reading from database...'.format(s))
        with engine.connect() as conn:
            df = pd.read_sql(sql=query.format(s, str(start_date)[:-3], str(end_date)[:-3]), con=conn)
            df = (df.rename(columns={'Timestamp':'TimeStamp'})
                    .assign(SignalID = df.SignalID.astype('int')))
    
        print('|{} creating cycles and detection events...'.format(s))
        c, d = etl_main(df, det_config_good)
        
        print('writing to files...')
        date_str = str(start_date)[:10]
        if not os.path.exists('../CycleData/' + date_str):
            os.mkdir('../CycleData/' + date_str)
        if not os.path.exists('../DetectionEvents/' + date_str):
            os.mkdir('../DetectionEvents/' + date_str)
            
        
        cd_file = '../CycleData/{}/cd_{}_{}.parquet'.format(date_str, s, date_str)
        de_file = '../DetectionEvents/{}/de_{}_{}.parquet'.format(date_str, s, date_str)
        
        c.to_parquet(cd_file) 
        d.to_parquet(de_file) 
        
        s3.upload_file(Filename=cd_file, 
                       Bucket='gdot-spm-cycles', 
                       Key='date={}/cd_{}_{}.parquet'.format(date_str, s, date_str))
        s3.upload_file(Filename=de_file, 
                       Bucket='gdot-spm-detections', 
                       Key='date={}/de_{}_{}.parquet'.format(date_str, s, date_str))
        
        os.remove(cd_file)
        os.remove(de_file)
        

        print('{}: {} seconds'.format(s, int(time.time()-t0)))
        
    
    except Exception as e:
        print(s, e)


        
        
    
if __name__=='__main__':

    t0 = time.time()
    
    engine = sq.create_engine('mssql+pyodbc://{}:{}@sqlodbc'.format(os.environ['ATSPM_USERNAME'], os.environ['ATSPM_PASSWORD']), pool_size=20)
    
    with engine.connect() as conn:

        det_config = pd.read_sql_table('DetectorConfig', con=conn)
        det_config = det_config.rename(columns={'CallPhase':'Call Phase'})
        
        bad_detectors = (pd.read_sql_table('BadDetectors', con=conn)
                            .assign(SignalID = lambda x: x.SignalID.astype('int64'),
                                    Detector = lambda x: x.Detector.astype('int64')))
        
    

    
    #corridors = pd.read_feather("GDOT-Flexdashboard-Report/corridors.feather")
    #signalids = list(corridors.SignalID.astype('int').values)
    
    with open('Monthly_Report_calcs.yaml') as yaml_file:
        conf = yaml.load(yaml_file)

    start_date = conf['start_date']
    end_date = conf['end_date']
    
    dates = pd.date_range(start_date, end_date, freq='1D')
    
    corridors_filename = conf['corridors_filename']
    corridors = pd.read_feather(corridors_filename)
    corridors = corridors[~corridors.SignalID.isna()]
    signalids = list(corridors.SignalID.astype('int').values)
    
    pool = Pool(24) #24
    asyncres = pool.starmap(etl2, list(itertools.product(signalids, dates)))
    pool.close()
    pool.join()
    

    
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    
    response = ath.start_query_execution(QueryString='MSCK REPAIR TABLE cycledata', 
                                         QueryExecutionContext={'Database': 'gdot_spm'},
                                         ResultConfiguration={'OutputLocation': 's3://aws-athena-query-results-322643905670-us-east-1'})
    response = ath.start_query_execution(QueryString='MSCK REPAIR TABLE detectionevents', 
                                         QueryExecutionContext={'Database': 'gdot_spm'},
                                         ResultConfiguration={'OutputLocation': 's3://aws-athena-query-results-322643905670-us-east-1'})
    
    print('\n{} signals in {} days. Done in {} minutes'.format(len(signalids), len(dates), int((time.time()-t0)/60)))

