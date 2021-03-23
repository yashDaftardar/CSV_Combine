import dask.dataframe as dd
import pandas as pd
import sys
from os import path

def combine(files):
    dataframes,files_new=[],[]
    sysout=sys.stdout
    
    # checked if the input has correct extension or nor, if the files exist or not or check extenion is correct or not
    for f in files:
        if f.endswith('.csv'):
            if path.exists(f):
                files_new.append(f)
            else:
                raise Exception("File does not exist" )
        else:
            raise Exception("incorrect file extension")
    
    #check there there is any csv file to combine or not
    if not files_new:
        raise Exception("No files to combine")
    
    #here the file is read, new column is added, converted to csv and then wrote on outfile 
    for f in files_new:
        file_name = f.split('/')[-1]
        df=dd.read_csv(f)
        df['filename']=file_name
        dataframes.append(df.compute())
    dataframes=pd.concat(dataframes)
    dataframes=dataframes.to_csv(index=False).replace("\r", "")
    sysout.write(dataframes)
    
if __name__ == '__main__':
    try:
        combine(sys.argv[1:])
    except Exception as e:
        print(e, file=sys.stderr)