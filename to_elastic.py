import os 
import mf_utils
import json
MAX_WORKERS = 8
MAX_MEMORY = 16000
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import logging

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
DATEFORMAT = '%Y-%m-%d-%H-%M-%S'


def create_logger(logger_name:str,level=logging.INFO):
    custom_logger = logging.getLogger(logger_name)
    ch1 = logging.StreamHandler()
    ch1.setLevel(level)
    ch1.setFormatter(formatter)
    custom_logger.addHandler(ch1)
    custom_logger.setLevel(level)    
    return custom_logger
    


INDEX="sem-scholar-index-3"
TYPE= "record"

SAVE_PROCESSED_DATA_PATH =os.path.join(mf_utils.data_path,'processed_data')
ONTOLOGY_CSV_PATH = os.path.join(SAVE_PROCESSED_DATA_PATH,'PageRankCollateFlow') 

# def doc_generator(df):
#     df_iter = df.iterrows()
#     for index, document in df_iter:
#         yield {
#                 "_index": 'sem-scholar-index',
#                 "_type": "_doc",
#                 "_id" : f"{document['id']}",
#                 "_source": document.to_dict(),
#             }
#     raise StopIteration

def rec_to_actions(df):
    import json
    for record in df.to_dict(orient="records"):
        # yield({'index': {'_id': record['id']}})
        yield (json.dumps(record))

def sync_data():
    logger = create_logger('ES_Import_Logger')
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers

    es = Elasticsearch(http_compress=True)

    present_folders = mf_utils.list_folders(f'processed_data/PageRankCollateFlow')
    s3_paths = [os.path.join(f,'ontology_processed.csv') for f in present_folders]
    for csv_df,pth in load_main_csvs(s3_paths):
        csv_df = clean_df(csv_df)
        actions = [
            {
                '_op_type': 'index',
                '_index': INDEX,
                '_type': 'doc',
                '_id': j['id'],
                '_source':j.to_dict()
            }
            for _,j in csv_df.iterrows()
        ]
        success,_ = helpers.bulk(es,actions,chunk_size=500,index=INDEX, doc_type='doc',stats_only=True,refresh=True )
        logger.info(f"Finished Flushing Data For {pth}")
        # for _,j in csv_df.iterrows():
        #     _id = j['id']
        #     # print(j.to_dict())
        #     es.index(index=INDEX,doc_type='_doc',id=_id,body=j.to_dict())

        # print(f"Finished Flushing Data For {pth}")
        # break


def clean_df(df):
    import numpy as np
    def parse_rows(x):
        x['ontology_enhanced'] = json.loads(x['ontology_enhanced'].replace("'",'"'))
        x['ontology_semantic'] = json.loads(x['ontology_semantic'].replace("'",'"'))
        x['ontology_syntactic'] = json.loads(x['ontology_syntactic'].replace("'",'"'))
        x['ontology_union'] = json.loads(x['ontology_union'].replace("'",'"'))
        try:
            x['authors'] = json.loads(x['authors'].replace("'",'"'))
        except:
            x['authors'] = []
        try:
            x['sources'] =  json.loads(x['sources'].replace("'",'"'))
        except:
            x['sources'] = []
        try:
            x['fieldsOfStudy'] = json.loads(x['authors'].replace("'",'"'))
        except:
            x['fieldsOfStudy'] = []
        x['inCitations'] = json.loads(x['inCitations'].replace("'",'"'))
        x['outCitations'] = json.loads(x['outCitations'].replace("'",'"'))
        try:
            x['pdfUrls'] = json.loads(x['pdfUrls'].replace("'",'"'))
        except:
            x['pdfUrls'] = []
        x['page_rank'] = x['page_rank'] * 10**8
        return x
    df1 = df.replace(np.nan, '', regex=True)
    df1 = df1[['id','entities', 'magId',
       'journalVolume', 'journalPages', 'pmid', 'fieldsOfStudy', 'year',
       'outCitations', 's2Url', 's2PdfUrl', 'authors', 'journalName',
       'paperAbstract', 'inCitations', 'pdfUrls', 'title', 'doi', 'sources',
       'doiUrl', 'venue', 'num_out_ctn', 'num_in_ctn', 'num_fields',
       'ontology_enhanced', 'ontology_semantic', 'ontology_syntactic',
       'ontology_union', 'page_rank']]
    df1 = df1.apply(lambda x:parse_rows(x),axis=1)

    return df1
        

def load_main_csvs(s3_paths):
    from metaflow import S3
    import pandas

    def form_df(pth):
        try:
            df = pandas.read_csv(pth.path)
            print(f"Retrieved Df for Key {pth.key}")
            return df
        except:
            print(f"Couldn't Extract Dataframe For {pth.key}")
            return None
    n = 0
    for pth in s3_paths:
        with S3(s3root=ONTOLOGY_CSV_PATH) as s3:
            s3_obj = s3.get(pth)
            df = form_df(s3_obj)
            if df is None:
                continue
            yield (df,pth)
        n+=1

if __name__ == '__main__':
    sync_data()