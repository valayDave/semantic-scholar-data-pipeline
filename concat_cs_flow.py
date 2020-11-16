from metaflow import FlowSpec,batch,step,Parameter
import mf_utils
import os 
PROCESSED_CS_PATH = os.path.join(mf_utils.data_path,'processed_data','CSDataExtractorFlow')
SAVE_PROCESSED_DATA_PATH =os.path.join(mf_utils.data_path,'processed_data')

class CSDataConcatFlow(FlowSpec):
    '''
    Concatenate Data From PROCESSED_CS_PATH to one DF
    '''

    sample = Parameter('sample',default=None,type=int,help=f'Use a sample of TAR Balls from {PROCESSED_CS_PATH}')

    chunk_size = Parameter('chunksize',default=2,type=int,help='Number of the Chunks To Process in Parallel for individual Foreach')

    @batch(cpu=16,memory=256000)
    @step
    def start(self):
        '''
        Join Df Here
        '''
        # s3_tar_paths = [ os.path.join('datasets','corpus_data',p) for p in mf_utils.list_folders('datasets/corpus_data',with_full_path=False)]
        s3_paths = [os.path.join(f,'category_citations.csv') for f in mf_utils.list_folders('processed_data/CSDataExtractorFlow',with_full_path=False)]
        if self.sample is not None:
            import random 
            s3_tar_paths = random.sample(s3_paths,self.sample)

        self.create_dataframe(s3_paths)        
        self.next(self.end)

    def create_dataframe(self,s3_paths):
        from metaflow import parallel_map,S3
        import pandas

        def form_df(pth):
            try:
                df = pandas.read_csv(pth.path)
                print(f"Retrieved Df for Key {pth.key}")
                return df
            except:
                print(f"Couldn't Extract Dataframe For {pth.key}")
                return None

        with S3(s3root=PROCESSED_CS_PATH) as s3:
            s3_objs = s3.get_many(s3_paths)
            print("Got the Data From S3. Now Concatenating")
            # final_dfs = parallel_map(lambda x: form_df(x),s3_objs)
            final_dfs =[]
            for x in s3_objs:
                add_df = form_df(x)
                if add_df is not None:
                    final_dfs.append(add_df)
            # concat_df = pandas.concat(list(filter(lambda x: x is not None,final_dfs)))
            final_dfs = pandas.concat(final_dfs)

        save_file_name = 'cs-concat-data.csv'
        final_dfs.to_csv(save_file_name)
        s3_save_path = os.path.join(
            SAVE_PROCESSED_DATA_PATH,self.__class__.__name__
        )
        with S3(s3root=s3_save_path) as s3:
            print("Saving Concat DF")
            df_save_path = s3.put_files([
                (
                    os.path.join(save_file_name),
                    save_file_name
                )
            ])[0]
        return df_save_path
            

    @step
    def end(self):
        print("Done Computation")


if __name__=='__main__':
    CSDataConcatFlow()