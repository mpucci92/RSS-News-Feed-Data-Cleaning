import pandas as pd
import glob

interesting_files = glob.glob("D:\\GoogleNews Data\\Merged CNBC\\*.csv")
df_list = []

for filename in sorted(interesting_files):
    df_list.append(pd.read_csv(filename))

full_df = pd.concat(df_list)

full_df.to_csv('D:\\GoogleNews Data\\ALL CNBC\\CNBC.csv')
