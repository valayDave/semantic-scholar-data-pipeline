## Load Data From Semscholar

1. ```!aws s3 cp --no-sign-request --recursive s3://ai2-s2-research-public/open-corpus/2020-05-27/ corpus_data```

2. Upload this Data To your Bucket which is specified in `~/.metaflowconfig/config.json` to a path like `$S3_PATH/datasets/semantics_scholar_corpus_data/
    ```python

    from metaflow import S3
    import os

    data_path = '/'.join(S3.get_root_from_config([]).split('/')[:-1])
    s3_root = os.path.join(data_path,'datasets')

    def sync_folder_to_s3(root_path,base_path='',s3_root=s3_root):
        sync_path = os.path.join(s3_root,base_path)
        
        file_paths = [(os.path.normpath(os.path.join(r,file)),os.path.join(r,file)) for r,d,f in os.walk(root_path) for file in f]
        # return file_paths
        for normpth,pth in file_paths:
        with S3(s3root=s3_root) as s3:
            file_paths = s3.put_files([(normpth,pth)])
            print(f"Finished Writing Path : {pth}")

        sync_path = os.path.join(
            sync_path,os.path.normpath(root_path
        ))
        return sync_path,file_paths
    sync_path,file_paths = sync_folder_to_s3('corpus_data',base_path='semantics_scholar_corpus_data')
    ```

## Flow Descriptions

1. [citation_harvest_flow.py](citation_harvest_flow.py) contains `SemScholarCorpusFlow` which will extract and remove all uncited material and saves it to 