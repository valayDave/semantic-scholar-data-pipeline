# Extracting CS Citation Graph From Semantic Scholar Corpus

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
The flow are follow the following pattern: 
1. Create clean data from large mound of semantic scholar cropus. Cleaning can be removing all uncited material.
2. Use the cleaned data to extract the CS based 
3. Use the CS based info to create Graph and classify ontology
4. Use graph to calc page rank and collate that with ontology results.
5. Dump this final processed information to elasticsearch 


### 1. SemScholarCorpusFlow
- [citation_harvest_flow.py](citation_harvest_flow.py) contains `SemScholarCorpusFlow` which will extract and remove all uncited material and saves dataframe for Each chunk under the Bucket `$CONFIG_S3_ROOT/processed_data/SemScholarCorpusFlow/s2-corpus-{i}/usefull_citations.csv`

### 2. CSDataExtractorFlow 
- `CSDataExtractorFlow` -----Requires-->`SemScholarCorpusFlow` 
- [cs_citation_extractor.py](cs_citation_extractor.py) contains `CSDataExtractorFlow` which will use the Chunks set by `SemScholarCorpusFlow` and extract `Computer Science` related Articles. It stores each chunk under the Bucket `$CONFIG_S3_ROOT/processed_data/CSDataExtractorFlow/s2-corpus-{i}/category_citations.csv`

### 3. CSDataConcatFlow
- `CSDataConcatFlow` ----Requires--> `CSDataExtractorFlow`
- [concat_cs_flow.py](concat_cs_flow.py) contains `CSDataConcatFlow` which concatenates the data into a single dataframe. This doesn't turn out that useful as csv becomes 22GB in size. The concat csv is stored at `$CONFIG_S3_ROOT/processed_data/CSDataConcatFlow/cs-concat-data.csv`. 

### 4. CSOntologyClassificationFlow
- `CSOntologyClassificationFlow` ----Requires--> `CSDataExtractorFlow`
- [ontology_classify_flow.py](ontology_classify_flow.py) contains `CSOntologyClassificationFlow`. It will use the [`cso_classifier`](https://github.com/angelosalatino/cso-classifier). `TODO: Push submodule which works as a relative import and Link it to repo.` This module will classify the data in the chunked csvs according to ontology described [here](https://cso.kmi.open.ac.uk/). The chunked dataframes are stored at `$CONFIG_S3_ROOT/processed_data/CSOntologyClassificationFlow/s2-corpus-{i}/ontology_processed.csv`

### 5. CSGraphBuilderFlow
- `CSGraphBuilderFlow` ----Requires--> `CSDataExtractorFlow`
- [ontology_classify_flow.py](ontology_classify_flow.py) contains `CSGraphBuilderFlow` which will use the `inCitations` and `outCitations` information from the chunked csv stored from `CSDataExtractorFlow` and store the graph to S3. The graph will be stored under 
`$CONFIG_S3_ROOT/processed_data/CSGraphBuilderFlow/citation_network_graph.json`

### 6. CSPageRankFinder
- `CSPageRankFinder` ----Requires--> `CSGraphBuilderFlow`
- [page_rank_flow.py](page_rank_flow.py) will use the graph stored via `CSGraphBuilderFlow` and performs page rank based on parameters. Finally stores the rank dictionary to `$CONFIG_S3_ROOT/processed_data/CSPageRankFinder/page-rank-{run-id}.json`

### 7. PageRankCollateFlow
- `PageRankCollateFlow` ----Requires--> `CSPageRankFinder`

- `PageRankCollateFlow` ----Requires--> `CSOntologyClassificationFlow`

- [collate_page_rank_flow.py](collate_page_rank_flow.py) contains the flow that will collate Page-Rank results from `CSPageRankFinder` flow and ontology classification results from `CSOntologyClassificationFlow`. This will finally store chunked csvs to `$CONFIG_S3_ROOT/processed_data/PageRankCollateFlow/s2-corpus-{i}/ontology_processed.csv`


## Utility Files

### to_elastic.py
- Extracts then csv chunks created by the `PageRankCollateFlow`(which have ontology and page rank collated results) and throws the data to elasticsearch. 

### mf_utils.py

- Utility module for data-syncing/s3 looksup and Dataroot path fixing when creating the flows. 

## TODO 

- Cleanup Readmes
- Add More information about running flows properly. 
- Create flow dependency graph
