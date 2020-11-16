from metaflow import FlowSpec,step,batch,Parameter,JSONType
import mf_utils
import os
import json
# PROCESSED_DATA_PATH = os.path.join(mf_utils.data_path,'processed_data','SemScholarCorpusFlow')
SAVE_PROCESSED_DATA_PATH =os.path.join(mf_utils.data_path,'processed_data')
S3_TAR_DATA_PATH = os.path.join(mf_utils.data_path,'datasets','corpus_data')

def clean_cat(x):
  try:
    return json.loads(str(x).replace("'",'"'))
  except:
    return []

CATEGORIES = '''
    - Art
    - Biology
    - Business
    - Chemistry
    - Computer Science
    - Economics
    - Engineering
    - Environmental Science
    - Geography
    - Geology
    - History
    - Materials Science
    - Mathematics
    - Medicine
    - Philosophy
    - Physics
    - Political Science
    - Psychology
    - Sociology

'''
def default_cat():
    return json.dumps(['Computer Science'])

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_ctndf_from_gz(ctn_file,categories=default_cat()):
    import pandas
    import gzip
    cite_data = []
    n=0
    with gzip.open(ctn_file,'r') as f :
        for l in f:
            dx = json.loads(l)
            if len(dx['inCitations']) > 0 or len(dx['outCitations']) > 0:
                n+=1
                cite_data.append(dx) # = [json.loads(l) for l in lines if journal_filter in l]
    return pandas.DataFrame(cite_data)


class CSDataExtractorFlow(FlowSpec):
    '''
    This flow will extract data according to selected categories. Uses Core Source Data to Extract Info based on Category array parameter. Default: ["Computer Science"]
    '''

    sample = Parameter('sample',default=None,type=int,help=f'Use a sample of TAR Balls from {S3_TAR_DATA_PATH}')

    selected_categories = Parameter(
        'categories',help=f'Selected Categories For the Flow from : {CATEGORIES}' ,type=JSONType,default=default_cat()
    )

    chunk_size = Parameter('chunksize',default=2,type=int,help='Number of the Chunks To Process in Parallel for individual Foreach')


    @step
    def start(self):
        '''
        Create Path chunks.
        '''
        # s3_tar_paths = [ os.path.join('datasets','corpus_data',p) for p in mf_utils.list_folders('datasets/corpus_data',with_full_path=False)]
        s3_tar_paths = mf_utils.list_folders('datasets/corpus_data',with_full_path=False)
        if self.sample is not None:
            import random 
            s3_tar_paths = random.sample(s3_tar_paths,self.sample)
        
        s3_tar_paths = list(set(s3_tar_paths) - set(['license.txt','sample-S2-records.gz','manifest.txt']))
        self.s3_tar_path_chunks = list(chunks(s3_tar_paths,self.chunk_size))
        self.next(self.process_chunk,foreach='s3_tar_path_chunks')
    

    @batch(cpu=16,memory=64000)
    @step
    def process_chunk(self):
        '''
        Parallely Process Path chunks and save category CSV df.
        '''
        from metaflow import parallel_map
        current_input_paths = self.input
        self.data_dfs_path = parallel_map(self.extract_individual_chunk,current_input_paths)
        self.next(self.join_paths)
    
    @step
    def join_paths(self,inputs):
        self.data_df_pths = []
        for i in inputs:
            self.data_df_pths.append(i.data_dfs_path)
        self.next(self.end)
    
    @step
    def end(self):
        print("Done Computation")

    
    def extract_individual_chunk(self,s3_chunk_url):
        from metaflow import S3
        import io
        s = io.StringIO()
        ss_df = None
        tar_file_name = s3_chunk_url.split('/')[-1].split('.gz')[0]
        cat_csv_name = f'category_citations-{tar_file_name}.csv'
        with S3(s3root=S3_TAR_DATA_PATH) as s3:
            s3_obj = s3.get(s3_chunk_url)
            print(f"Extracted S3 Data {s3_obj.path}")
            ss_df = get_ctndf_from_gz(s3_obj.path,categories=self.selected_categories)
            ss_df['num_out_ctn'] = ss_df['outCitations'].apply(lambda x: len(x))
            ss_df['num_in_ctn'] = ss_df['inCitations'].apply(lambda x: len(x))
            save_df = self.extract_cat_df(ss_df)
            print(f"Extracted UseFul Information {len(save_df)}")
            # csv_str = s.getvalue()
            save_df.to_csv(cat_csv_name)
            
        print("Now Starting Uploading Of Parsed Data")
        s3_save_path = os.path.join(
            SAVE_PROCESSED_DATA_PATH,self.__class__.__name__,tar_file_name
        )
        with S3(s3root=s3_save_path) as s3:
            print("Saving Metadata")
            df_save_paths = s3.put_files([
                (
                    os.path.join('category_citations.csv'),
                    cat_csv_name
                )
            ])[0]
            print(f"Saved Metadata {s3_save_path}")
            return_object = dict(
                df_save_path = df_save_paths[1]
            )

        return return_object

    @staticmethod
    def extract_cat_df(ss_df,categories=json.loads(default_cat())):
        from metaflow import S3
        import pandas
        df_data = ss_df
        
        def filter_cats(x,cats):
            for c in cats:
                for x1 in x: 
                    if c == x1:
                        return True
            return False
        
        df_data['fieldsOfStudy'] = df_data['fieldsOfStudy'].apply(lambda x:clean_cat(x))
        print(f"Size of Data DF : {len(df_data)}")
        df_data['num_fields'] = df_data['fieldsOfStudy'].apply(lambda x:len(x))
        filtered_df_data = df_data[df_data['num_fields'] > 0]
        print(f"Size of filtered_df_data : {len(filtered_df_data)} And Extracting Category : {categories}")
        flat_set = set([item for sublist in df_data['fieldsOfStudy'].values for item in sublist])
        print(f"Avaliable Categories in Df : {flat_set}")
        cat_filtered_df = filtered_df_data[filtered_df_data['fieldsOfStudy'].apply(lambda x : filter_cats(x,categories))]
        print(f"Size of filtered_df_data : {len(cat_filtered_df)}")
        return cat_filtered_df



if __name__=='__main__':
    CSDataExtractorFlow()