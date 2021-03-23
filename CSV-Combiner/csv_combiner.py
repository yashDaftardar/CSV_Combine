import dask.dataframe as dd
import pandas as pd
import sys
import os.path
from os import path
def combine(files):
    dataframes,files_new=[],[]
    sysout=sys.stdout
    for f in files:
        if f.endswith('.csv'):
            if path.exists(f):
                files_new.append(f)
            else:
                raise Exception("File does not exist" )
        else:
            raise Exception("incorrect file extension")
    if not files_new:
        raise Exception("No files to combine")

    for f in files_new:
        file_name = f.split('/')[-1]
        df=dd.read_csv(f)
        df['filename']=file_name
        dataframes.append(df.compute())
    dataframes=pd.concat(dataframes)
    result=dataframes.to_csv(index=False).replace("\r", "")
    sysout.write(result)
    
if __name__ == '__main__':
    try:
        combine(sys.argv[1:])
    except Exception as e:
        print(e, file=sys.stderr)