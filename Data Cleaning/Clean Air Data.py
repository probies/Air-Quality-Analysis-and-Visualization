import pandas as pd
import os

df=pd.read_csv("C:/Users/jhapi/Desktop/Dataset/db2.csv")
print (df)
bf = df.dropna(axis='columns', how='all')
index_with_nan = bf.index[bf.isnull().any(axis=1)]
index_with_nan.shape
bf.drop(index_with_nan,0, inplace=True)
bf.shape
print (bf)

bf.to_csv("C:/Users/jhapi/Desktop/Dataset/Air/db2.csv" , index=False)