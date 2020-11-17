import os 
import mf_utils
import json
MAX_WORKERS = 8
MAX_MEMORY = 16000
from elasticsearch import Elasticsearch
from elasticsearch import helpers


INDEX="sem-scholar-index-2"
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
                **j.to_dict()
            }
            for _,j in csv_df.iterrows()
        ]
        success,_ = helpers.bulk(es,actions,chunk_size=100,index=INDEX, doc_type='doc',stats_only=True,refresh=True )
        print(f"Finished Flushing Data For {pth}")
        print(f"SUccess Measure {success}")
        break


def clean_df(df):
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
        return x
    df['_id'] = df['id']
    df = df.apply(lambda x:parse_rows(x),axis=1)
    return df
        

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