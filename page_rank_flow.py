from metaflow import FlowSpec,step,Parameter,conda,batch,current
import os 
import mf_utils
import json
MAX_WORKERS = 16
MAX_MEMORY = 128000

SAVE_PROCESSED_DATA_PATH =os.path.join(mf_utils.data_path,'processed_data')
PROCESSED_CS_PATH = os.path.join(SAVE_PROCESSED_DATA_PATH,'CSGraphBuilderFlow') # Use Chunks for ease of Computations
CONDA_DEPS = {
    'networkx':'2.5'
}
from functools import reduce
# class ContentGraph:



class CSPageRankFinder(FlowSpec):
    '''
    Build Citation Graph as JSON From Dataset. 
    Save To S3. 

    Use For Calculating Page Rank Later. 
    '''
    tolerence = Parameter('tolerence',default=1e-8,help='Error Tolerance for Page Conversion')

    max_iter = Parameter('max_iter',default=100,help='Max Iterations to Run')
    
    @batch(cpu=16,memory=256000)
    @conda(python='3.7.2',libraries=CONDA_DEPS)
    @step
    def start(self):
        graph_json = self.load_graph()
        import networkx as nx 
        import page_rank
        G = nx.from_dict_of_dicts(graph_json,create_using=nx.DiGraph)
        print(f"Size of the Graph is {len(graph_json)}")
        del graph_json
        rank_dict,err_log = page_rank.pagerank(
            G,tol=self.tolerence,max_iter=self.max_iter
        )
        self.error_log = err_log
        self.rank_save_path = self.save_json(rank_dict,save_name=f'page-rank-{current.run_id}.json')
        print(f"Saved Rank at {self.rank_save_path}")
        self.next(self.end)
     
    def save_json(self,data_json,tmp_pth = 'temp_save.json',save_name='data.json'):
        from metaflow import S3
        import shutil
        final_path = os.path.join(
            SAVE_PROCESSED_DATA_PATH,self.__class__.__name__
        )
        with S3(s3root=final_path) as s3:
            print(f"Saving data_json To S3")
            with open(tmp_pth,'w') as f:
                json.dump(data_json,f)
            put_pth = s3.put_files(
                [(save_name,tmp_pth)]
            )[0][1]
            
        return put_pth   
    
    def load_graph(self):
        from metaflow import S3
        import json
        with S3(s3root=PROCESSED_CS_PATH) as s3:
            s3_resp = s3.get('citation_network_graph.json')
            return json.loads(s3_resp.text)

    @step
    def end(self):
        print("Done Computation")

if __name__ == "__main__":
    CSPageRankFinder()