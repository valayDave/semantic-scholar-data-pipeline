from metaflow import S3
import os

def safe_mkdir(pth):
    try:
        os.makedirs(pth)
    except:
        return 

data_path = '/'.join(S3.get_root_from_config([]).split('/')[:-1])
s3_root = os.path.join(data_path,'datasets')

def sync_folder_to_s3(root_path,base_path='',s3_root=s3_root):
    sync_path = os.path.join(s3_root,base_path)
    
    file_paths = [(os.path.normpath(os.path.join(r,file)),os.path.join(r,file)) for r,d,f in os.walk(root_path) for file in f]

    with S3(s3root=s3_root) as s3:
        s3.put()
        file_paths = s3.put_files(file_paths)

    sync_path = os.path.join(
        sync_path,os.path.normpath(root_path
    ))
    return sync_path,file_paths

def sync_folder_from_bucket(bucket_path,folder_path):
    safe_mkdir(folder_path)
    with S3(s3root=bucket_path) as s3:
        for resp in s3.get_all():
            dir_path = os.path.join(
                folder_path,
                os.path.dirname(
                    resp.key
                ),
            )
            file_path = os.path.join(
                folder_path,
                resp.key
            )
            safe_mkdir(dir_path)
            print("Writing File To : %s",file_path)
            with open(file_path,'wb+') as f:
                f.write(resp.blob)
    return folder_path

def list_folders(base_path,s3_root=data_path,with_full_path=False):
    if base_path == '/': base_path = ''
    sync_path = os.path.join(s3_root,base_path)
    pths = []
    with S3(s3root=sync_path) as s3:
        for resp in s3.list_paths():
            if with_full_path:
                pths.append(os.path.join(sync_path,resp.key))
            else:
                pths.append(resp.key)

    return pths

