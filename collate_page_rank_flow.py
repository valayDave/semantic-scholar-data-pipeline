from metaflow import FlowSpec,step,Parameter,conda,batch,current
import os 
import mf_utils
import json
MAX_WORKERS = 16
MAX_MEMORY = 128000

SAVE_PROCESSED_DATA_PATH =os.path.join(mf_utils.data_path,'processed_data')
PAGE_RANK_PATH = os.path.join(SAVE_PROCESSED_DATA_PATH,'CSPageRankFinder') 
ONTOLOGY_CSV_PATH = os.path.join(SAVE_PROCESSED_DATA_PATH,'CSOntologyClassificationFlow') 


CONDA_DEPS = {
    'networkx':'2.5'
}
from functools import reduce
# class ContentGraph:



class PageRankCollateFlow(FlowSpec):
    '''
    Build Citation Graph as JSON From Dataset. 
    Save To S3. 

    Use For Calculating Page Rank Later. 
    '''

    sample = Parameter('sample',default=None,type=int,help=f'Use a sample of for data in each df')
    
    @batch(cpu=16,memory=256000)
    @step
    def start(self):
        present_folders = mf_utils.list_folders(f'processed_data/{self.__class__.__name__}')
        s3_paths = [os.path.join(f,'ontology_processed.csv') for f in mf_utils.list_folders('processed_data/CSOntologyClassificationFlow',with_full_path=False) if f not in present_folders]
        if self.sample is not None:
            import random 
            s3_path = random.sample(s3_paths,sample)
        self.save_df_paths = []
        self.collate_dfs_and_rank(s3_path)
        self.next(self.end)

    def collate_dfs_and_rank(self,s3_paths):
        page_rank_json = self.load_json('citation_network_graph.json',PAGE_RANK_PATH)
        def collate_pr(idv,prjs):
            if idv in prjs:
                return prjs[idv]
            else:
                return 0

        for csv_df,pth in self.load_main_csvs(s3_paths):
            print(f"Extracting For Set {pth}")
            csv_df['page_rank'] = csv_df['id'].apply(lambda x: collate_pr(x,page_rank_json))
            save_pth = self.save_data_df(csv_df,pth)
            self.save_df_paths.append(save_pth)

    def save_data_df(self,df,from_pth):
        from metaflow import S3
        import shutil
        partition_name = from_pth.split('/')[-2]
        final_path = os.path.join(
            SAVE_PROCESSED_DATA_PATH,self.__class__.__name__,partition_name
        )
        with S3(s3root=final_path) as s3:
            print(f"Data Frame Saved To S3 With Mined Ontology AND RANK {partition_name}")
            temp_save_pth = f'{partition_name}-temp.csv'
            df.to_csv(temp_save_pth)
            s3_save_dest = s3.put_files([(
                'ontology_processed.csv',temp_save_pth
            )])[0][1]
            os.remove(temp_save_pth)
        return s3_save_dest

    
    def load_json(self,json_file_name,s3root):
        from metaflow import S3
        import json
        with S3(s3root=s3root) as s3:
            s3_resp = s3.get(json_file_name)
            return json.loads(s3_resp.text)
    
    
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
            with S3(s3root=ONTOLOGY_CSV_PATH) as s3:
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

if __name__ == "__main__":
    PageRankCollateFlow()