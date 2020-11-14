import mf_utils
import os 
import json
from metaflow import Parameter,FlowSpec,step,batch
S3_TAR_DATA_PATH = os.path.join(mf_utils.data_path,'datasets','corpus_data')


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]




class SemScholarCorpusFlow(FlowSpec):
    sample = Parameter('sample',default=None,type=int,help=f'Use a sample of TAR Balls from {S3_TAR_DATA_PATH}')

    chunk_size = Parameter('chunksize',default=2,type=int,help='Number of the Chunks To Process in Parallel for individual Foreach')

    @step
    def start(self):
        # s3_tar_paths = [ os.path.join('datasets','corpus_data',p) for p in mf_utils.list_folders('datasets/corpus_data',with_full_path=False)]
        s3_tar_paths = mf_utils.list_folders('datasets/corpus_data',with_full_path=False)
        if self.sample is not None:
            import random 
            s3_tar_paths = random.sample(s3_tar_paths,self.sample)
        
        self.s3_tar_path_chunks = list(chunks(s3_tar_paths,self.chunk_size))
        self.next(self.process_chunk,foreach='s3_tar_path_chunks')
    
    @batch(cpu=4,memory=16000)
    @step
    def process_chunk(self): # Todo: Add Conda Deps to this. 
        '''
        This will be a foreach process where each Chunk's Following data will be extracted: 
            1. useful citation(content with citn in/out)
            2. num in citation
            2. num out citation
        Additionally a CSV of the DF will be stored in the S3 Repo with one json representing the following:
            {
                useful_ids:set(),
                in_citn:set(),
                out_citn:set(),
            }
        '''
        from metaflow import parallel_map
        s3_paths = self.input 
        self.chunk_dicts = parallel_map(self.extract_individual_chunk,s3_paths)
        self.next(self.join_citations)
    
    @step
    def join_citations(self,inputs):
        self.useful_ids = set()
        self.in_citn = set()
        self.out_citn = set()
        for inp in inputs:
            for c in inp.chunk_dicts:
                self.useful_ids.update(c['citation_meta_object']['citation_ids'])
                self.in_citn.update(c['citation_meta_object']['in_citations'])
                self.out_citn.update(c['citation_meta_object']['out_citations'])
        
        self.next(self.end)
    
    def extract_individual_chunk(self,s3_chunk_url):
        from metaflow import S3
        import io
        with S3(s3root=S3_TAR_DATA_PATH) as s3:
            s3_obj = s3.get(s3_chunk_url)
            ss_df = self.get_ctndf_from_gz(s3_obj.path)
            ss_df['num_out_ctn'] = ss_df['outCitations'].apply(lambda x: len(x))
            ss_df['num_in_ctn'] = ss_df['inCitations'].apply(lambda x: len(x))
            useful_df =  ss_df[~ss_df.apply(lambda row:row['num_in_ctn'] == 0 and row['num_out_ctn'] == 0,axis=1)]
            flat_in_ids = set([item for sublist in useful_df['inCitations'].values for item in sublist])
            flat_out_ids = set([item for sublist in useful_df['outCitations'].values for item in sublist])
            present_ids = set(useful_df['id'])
            s = io.StringIO()
            useful_df.to_csv(s)
            csv_str = s.getvalue()
            
            citation_meta_object = dict(
                citation_ids = present_ids,in_citations=flat_in_ids,out_citations=flat_out_ids
            )
            tar_file_name = s3_chunk_url.split('/')[-1].split('.gz')[0]
            s3_save_path = os.path.join(
                mf_utils.data_path,'processed_data',self.__class__.__name__,tar_file_name
            )
            df_save_path = s3.put( # Add the Citation File. 
                os.path.join(
                    s3_save_path,'usefull_citations.csv'
                ),csv_str
            )
            meta_save_path = s3.put(
                os.path.join(
                    s3_save_path,'citation_info.json'
                ),json.dumps(citation_meta_object)
            )
            return_object = dict(
                meta_save_path = meta_save_path,
                df_save_path = df_save_path,
                citation_meta_object = citation_meta_object,
            )
            return return_object
    
    @step
    def end(self):
        print("Done Computation")

    
    @staticmethod
    def get_ctndf_from_gz(ctn_file,doc_filter=None):
        import pandas
        import gzip
        cite_data = []
        with gzip.open(ctn_file,'r') as f :
            for l in f:
                dx = json.loads(l)
                cite_data.append(dx) # = [json.loads(l) for l in lines if journal_filter in l]
        return pandas.DataFrame(cite_data)

if __name__=='__main__':
    SemScholarCorpusFlow()