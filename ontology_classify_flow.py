from metaflow import FlowSpec,step,Parameter,conda,batch
import os 
import mf_utils
CONDA_DEPS = {
    'hurry.filesize':'0.9',
    'kneed':'0.4.1',
    'nltk':'3.4.1',
    'pandas':'0.24.2',
    'python-levenshtein':'0.12.0',
    'requests':'2.22.0',
    'spacy':'2.2.3', 
    'nltk_data':'2017.10.22',
    'spacy-model-en_core_web_sm':'2.2.0'
}
MAX_WORKERS = 8
MAX_MEMORY = 16000

SAVE_PROCESSED_DATA_PATH =os.path.join(mf_utils.data_path,'processed_data')
PROCESSED_CS_PATH = os.path.join(SAVE_PROCESSED_DATA_PATH,'CSDataExtractorFlow') # Use Chunks for ease of Computations

class CSOntologyClassificationFlow(FlowSpec):
    
    max_chunks = Parameter('max_chunks',default=None,type=int,help=f'Number of Chunks to Process for the DF Stored in {PROCESSED_CS_PATH}')

    chunksize = Parameter('chunksize',type=int,default=6,help='Size of the Chunks to read from the DataFrame. Expressed as int but performs 10^chunksize')

    @batch(cpu=MAX_WORKERS,memory=MAX_MEMORY)
    @conda(python='3.7.2',libraries=CONDA_DEPS)
    @step
    def start(self):
        s3_paths = [os.path.join(f,'category_citations.csv') for f in mf_utils.list_folders('processed_data/CSDataExtractorFlow',with_full_path=False)]
        self.save_df_paths = []
        for csv_df,pth in self.load_main_csvs(s3_paths):
            svpth = self.df_iterator(csv_df,pth)
            self.save_df_paths.append(svpth)

        self.next(self.end)


    def df_iterator(self,csv_df,pth):
        data_df = csv_df.dropna(subset=['id','title']).drop_duplicates(subset=['id'])
        data_df = self.classify_ontology(data_df)
        df_save_path = self.save_data_df(data_df,pth)
        return df_save_path

    def save_data_df(self,df,from_pth):
        from metaflow import S3
        import shutil
        partition_name = from_pth.split('/')[-2]
        final_path = os.path.join(
            SAVE_PROCESSED_DATA_PATH,self.__class__.__name__,partition_name
        )
        with S3(s3root=final_path) as s3:
            print(f"Data Frame Saved To S3 With Mined Ontology {partition_name}")
            temp_save_pth = f'{partition_name}-temp.csv'
            df.to_csv(temp_save_pth)
            s3_save_dest = s3.put_files([(
                'ontology_processed.csv',temp_save_pth
            )])[0][1]
            os.remove(temp_save_pth)
        return s3_save_dest


    def classify_ontology(self,df):
        import cso_classifier.classifier.classifier as CSO
        import json
        from metaflow import parallel_map
        from functools import reduce
        import pandas
        model_identity_dict = dict()
        # json_df_data = json.loads(df.to_json(orient='records'))
        id_indxd_df = df[['id','paperAbstract','title']].set_index('id')
        da_js = json.loads(id_indxd_df.rename(columns={'paperAbstract':'abstract'}).to_json(orient='index'))
        ont_result = CSO.run_cso_classifier_batch_mode(da_js,workers=MAX_WORKERS)
        renamed_cols={
            "syntactic": "ontology_syntactic",
            "semantic": "ontology_semantic",
            "union": "ontology_union",
            "enhanced": "ontology_enhanced",
            "explanation": 'ontology_explanation'
        }
        ont_df = pandas.read_json(ont_result,orient='index').rename(columns=renamed_cols)
        ont_df.index.name = 'id'
        df = df.join(ont_df)
        return df
        
    def load_main_csvs(self,s3_paths):
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
            with S3(s3root=PROCESSED_CS_PATH) as s3:
                s3_obj = s3.get(pth)
                yield (pandas.read_csv(s3_obj.path),pth)
            if self.max_chunks is not None:
                if self.max_chunks < n:
                    break     

    @step
    def end(self):
        print("Done Computation")


    @staticmethod
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

if __name__ =='__main__':
    CSOntologyClassificationFlow()