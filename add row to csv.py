import pandas as pd
import os
from pathlib import Path
import numpy as np

#in 100% 200pkt toglilo
path = Path(r"B:/Risultati Scaling/")

dirs = os.listdir(path)
for dir in dirs:
    if dir=="base" or dir=="no scaling pod up":
        continue

    timestamp_collection = Path.joinpath(path,dir, r"continuous scaling\csv_results\correct.csv")
    file_500_csv = Path.joinpath(path,dir, r"500 pkt scaling\csv_results\correct.csv")
    file_200_csv = Path.joinpath(path,dir, r"200 pkt scaling\csv_results\correct.csv")

    df = pd.read_csv(timestamp_collection)
    df_500 = pd.read_csv(file_500_csv)
    df_200 = pd.read_csv(file_200_csv)

    diff_last_rows_request = df["Request scaling time(ns)"].iloc[-1] - df["Request scaling time(ns)"].iloc[-2]
    diff_req_stop = df["Service stop responding time (ns)"].iloc[-1] - df["Request scaling time(ns)"].iloc[-1]

    request_count_500 = 27 * diff_last_rows_request + df_500["Request scaling time(ns)"].iloc[-1]
    request_count_200 = 94 * diff_last_rows_request + df_200["Request scaling time(ns)"].iloc[-1]

    series_500 = ['UP',np.int64(2324), np.int64(168), request_count_500, request_count_500+diff_req_stop, diff_req_stop]
    series_200 = ['UP',np.int64(2324), np.int64(168), request_count_200, request_count_200+diff_req_stop, diff_req_stop]

    df_500.loc[len(df_500.index)] = series_500
    df_200.loc[len(df_200.index)] = series_200

    filename_500 = Path.joinpath(path,dir, r"500 pkt scaling\csv_results\scaled_new.csv")
    df_500.to_csv(filename_500, index=False, encoding='utf-8')
    if(dir=="25 percent CPU"):
        filename_200 = Path.joinpath(path,dir, r"200 pkt scaling\csv_results\scaled_new.csv")
        df_200.to_csv(filename_200, index=False, encoding='utf-8')

    print('done')
