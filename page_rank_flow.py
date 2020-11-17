from metaflow import FlowSpec,step,Parameter,conda,batch
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
    
    @batch(cpu=16,memory=64000)
    @step
    def start(self):
        graph_json = self.load_graph()
        print(f"Size of the Graph is {len(graph_json)}")
        self.next(self.end)
    
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