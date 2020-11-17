from metaflow import FlowSpec,step,Parameter,conda,batch,JSONType
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
import json
MAX_WORKERS = 32
MAX_MEMORY = 160000

SAVE_PROCESSED_DATA_PATH =os.path.join(mf_utils.data_path,'processed_data')
PROCESSED_CS_PATH = os.path.join(SAVE_PROCESSED_DATA_PATH,'CSDataExtractorFlow') # Use Chunks for ease of Computations

def default_cat():
    return json.dumps([

    ])

class CSOntologyClassificationFlow(FlowSpec):
    
    max_chunks = Parameter('max_chunks',default=None,type=int,help=f'Number of Chunks to Process for the DF Stored in {PROCESSED_CS_PATH}')

    chunksize = Parameter('chunksize',type=int,default=6,help='Size of the Chunks to read from the DataFrame. Expressed as int but performs 10^chunksize')

    sample = Parameter('sample',default=None,type=int,help=f'Use a sample of for data in each df')

    print_counter =  Parameter('print_counter',default=100,type=int,help=f'How many times to check when printing')

    ignore_set = Parameter(
        'ignore_set',help=f'Folders To ignore From : {PROCESSED_CS_PATH}' ,type=JSONType,default=default_cat()
    )


    @batch(cpu=MAX_WORKERS,memory=MAX_MEMORY)
    @conda(python='3.7.2',libraries=CONDA_DEPS)
    @step
    def start(self):
        present_folders = mf_utils.list_folders(f'processed_data/{self.__class__.__name__}')
        if len(self.ignore_set) > 0:
            print(f"Ignoring Paths : {self.ignore_set}")
            s3_paths = [os.path.join(f,'category_citations.csv') for f in mf_utils.list_folders('processed_data/CSDataExtractorFlow',with_full_path=False) if f not in self.ignore_set]    
        else:
            print(f"Ignoring Paths : {present_folders}")
            s3_paths = [os.path.join(f,'category_citations.csv') for f in mf_utils.list_folders('processed_data/CSDataExtractorFlow',with_full_path=False) if f not in present_folders]
        self.save_df_paths = []
        for csv_df,pth in self.load_main_csvs(s3_paths):
            print(f"Extracting For Set {pth}")
            svpth = self.df_iterator(csv_df,pth)
            self.save_df_paths.append(svpth)

        self.next(self.end)


    def df_iterator(self,csv_df,pth):
        data_df = csv_df.dropna(subset=['id','title']).drop_duplicates(subset=['id'])
        if self.sample is not None:
            data_df = data_df.sample(n=self.sample)
        data_df = self.classify_ontology(data_df,pth)
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


    def classify_ontology(self,df,from_pth):
        import cso_classifier.classifier.classifier as CSO
        import json
        from metaflow import parallel_map, S3
        from functools import reduce
        import pandas
        model_identity_dict = dict()
        partition_name = from_pth.split('/')[-2]
        # json_df_data = json.loads(df.to_json(orient='records'))
        id_indxd_df = df[['id','paperAbstract','title']].set_index('id')
        da_js = json.loads(id_indxd_df.rename(columns={'paperAbstract':'abstract'}).to_json(orient='index'))
        ont_result = CSO.run_cso_classifier_batch_mode(da_js,workers=MAX_WORKERS,print_counter=self.print_counter)
        renamed_cols={
            "syntactic": "ontology_syntactic",
            "semantic": "ontology_semantic",
            "union": "ontology_union",
            "enhanced": "ontology_enhanced",
            "explanation": 'ontology_explanation'
        }
        ont_df = pandas.read_json(json.dumps(ont_result),orient='index').rename(columns=renamed_cols)
        ont_df.index.name = 'id'
        df = pandas.concat((df.set_index('id'),ont_df),axis=1)
        final_path = os.path.join(
            SAVE_PROCESSED_DATA_PATH,self.__class__.__name__,partition_name
        )
        with S3(s3root=final_path) as s3:
            rss = s3.put('ontology.json',json.dumps(ont_result))
            print(f"Ontology Saved At {rss}")

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
                df = form_df(s3_obj)
                if df is None:
                    continue
                yield (df,pth)
            n+=1
            if self.max_chunks is not None:
                if self.max_chunks < n:
                    return     

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