import pandas as pd
import openpyxl


src_file_path="/py_temp/"

df = pd.read_excel(src_file_path+'PowerPivot.xls')
print(df)