from metaflow import FlowSpec,step,Parameter,conda,batch
import os 
import mf_utils
import json
MAX_WORKERS = 16
MAX_MEMORY = 128000

SAVE_PROCESSED_DATA_PATH =os.path.join(mf_utils.data_path,'processed_data')
PROCESSED_CS_PATH = os.path.join(SAVE_PROCESSED_DATA_PATH,'CSDataExtractorFlow') # Use Chunks for ease of Computations
CONDA_DEPS = {
    'networkx':'2.5'
}
from functools import reduce
# class ContentGraph:




class CSGraphBuilderFlow(FlowSpec):
    '''
    Build Citation Graph as JSON From Dataset. 
    Save To S3. 

    Use For Calculating Page Rank Later. 
    '''
    
    sample = Parameter('sample',default=None,type=int,help=f'Use a sample of for Paths for Loading Dfs')

    # @conda(python='3.7.2',libraries=CONDA_DEPS)
    @batch(cpu=MAX_WORKERS,memory=MAX_MEMORY)
    @step
    def start(self):
        s3_paths = [os.path.join(f,'category_citations.csv') for f in mf_utils.list_folders('processed_data/CSDataExtractorFlow',with_full_path=False)]
        if self.sample is not None:
            import random
            s3_paths = random.sample(s3_paths,self.sample)
        print(f'Building Network Graph From {len(s3_paths)}')
        self.build_graph(s3_paths)
        self.next(self.end)


    @staticmethod
    def in_cite_reducer(acc,memo):
        loaded_memo = memo
        for k in loaded_memo:
            if k in acc:
                acc[k].update(memo[k])
            else:
                acc[k] = memo[k]
        return acc

    def build_graph(self,s3_paths):
        final_json_graph = {}
        for csv_df,pth in self.load_main_csvs(s3_paths):
            data_df = csv_df.dropna(subset=['id','title']).drop_duplicates(subset=['id'])
            incite_js,out_citejs = self.create_citation_jsons(data_df)
            print(f"Performing Reduction For {pth} With Graph Of Size {len(final_json_graph)}")
            final_json_graph = self.in_cite_reducer(self.in_cite_reducer(final_json_graph,out_citejs),incite_js)
            pth = self.save_graph(final_json_graph,save_name=f'citation_network_graph-{pth.split("/")[-2]}.json')
            print(f"Graph Saved At : {pth}")

        print("Finished Collecting Nodes.!")



    def create_citation_jsons(self,ss_df):
        def change_out_cite(x): 
            try:
                cite_arr = json.loads(x.replace("'",'"'))
                op = {}
                wt = {'weight':1}
                for c in cite_arr:
                    op[c] = wt
                return json.dumps(op)
            except: 
                return json.dumps({})

        def change_in_cite(x,id):
            try:
                cite_arr = json.loads(x.replace("'",'"'))
                wt = {'weight':1}
                p = {}
                for c in cite_arr:
                    p[c] = {}
                    p[c][id] = wt
                return json.dumps(p)
            except: 
                return json.dumps({})

        dtdf = ss_df[['id','outCitations','inCitations']]

        dtdf['outCitations_js'] = dtdf['outCitations'].apply(lambda x: change_out_cite(x))
        dtdf['inCitations_js']= dtdf.apply(lambda x: change_in_cite(x['inCitations'],x['id']),axis=1)
        octtn_arr =json.loads(dtdf[['outCitations_js','id']].to_json(orient='records'))
        in_citearr = [json.loads(i) for i in json.loads(dtdf['inCitations_js'].to_json(orient='records'))]

        def out_cite_reducer(acc,memo):
            acc[memo['id']] = json.loads(memo['outCitations_js'])
            return acc

        incite_js = reduce(self.in_cite_reducer,in_citearr,{})
        out_citejs = reduce(out_cite_reducer,octtn_arr,{})
        return (incite_js,out_citejs)


    def save_graph(self,graph_json,tmp_pth = 'temp_save_graph.json',save_name='citation_network_graph.json'):
        from metaflow import S3
        import shutil
        final_path = os.path.join(
            SAVE_PROCESSED_DATA_PATH,self.__class__.__name__
        )
        print("ABOUT TO SAVE THE GRAPH !")
        with S3(s3root=final_path) as s3:
            print(f"Saving Graph To S3")
            with open(tmp_pth,'w') as f:
                json.dump(graph_json,f)
            put_pth = s3.put_files(
                [(save_name,tmp_pth)]
            )[0][1]
            
        return put_pth

       
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
        for idx,pth in enumerate(s3_paths):
            with S3(s3root=PROCESSED_CS_PATH) as s3:
                s3_obj = s3.get(pth)
                df = form_df(s3_obj)
                if df is None:
                    if idx < len(s3_paths) -2:
                        continue
                    else:
                        return
                yield (df,pth)
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
    CSGraphBuilderFlow()