import pandas as pd
import os
from pathlib import Path

limits_file = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling Countinus/1h/csv_results/correct.csv"
directory_timestamps_states = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling Countinus/1h"
dir_results = ['results_state', 'results_cpus']
path = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling Countinus/1h/cstate_results"

mean=True
df_limits = pd.read_csv(limits_file)

count = 0
a = df_limits['Sequence number'].to_list()
limit_timestamp = []
for i in range(len(a) - 1):
    if a[i + 1] == a[i] + 1:  # check if in the same window
        count += 1
        continue
    else:
        start = df_limits.loc[i - count, 'Receiving time (ns)']
        end = df_limits.loc[i, 'Receiving time (ns)']
        limit_timestamp.append((start, end))
        count = 0

# get all files in dir
for dir in dir_results:
    temp=Path(directory_timestamps_states,dir)
    file_list = [f for f in os.listdir(temp) if
                 os.path.isfile(os.path.join(temp, f))]

    t_start = limit_timestamp[0][0]
    t_end = limit_timestamp[-1][1]
    save_dir = Path(path + "/" + dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    for file in file_list:
        df_timestamps = pd.read_csv(Path(temp,file))
        df_columns = df_timestamps.columns.to_list()
        df_columns.insert(0, "Scale")
        df_new = pd.DataFrame(columns=df_columns)
        df_mean_active = []
        df_mean_passive = []
        timestamps = df_timestamps["Unnamed: 0"].to_list()
        current_window = 0
        count_fin_attiva = 0
        count_fin_passiva = 0
        count_eliminate = 0
        for index, timest in enumerate(timestamps):
            if timest < t_start or timest > t_end:
                count_eliminate += 1
                continue
            else:
                if timest >= limit_timestamp[current_window][0] and timest <= limit_timestamp[current_window][1]:
                    #finestra n attiva
                    print("campionamento finestra attiva")
                    count_fin_attiva += 1
                    series_temp = pd.concat([pd.Series({'Scale': 1}), df_timestamps.loc[index]])
                    df_new.loc[len(df_new)] = series_temp
                    #df_mean_active.append(df_timestamps.iloc[index,1:])
                elif current_window + 1 < len(limit_timestamp):
                    #finestra n passiva
                    if timest > limit_timestamp[current_window][1] and timest < limit_timestamp[current_window + 1][0]:
                        count_fin_passiva += 1
                        series_temp = pd.concat([pd.Series({'Scale': 0}), df_timestamps.loc[index]])
                        df_new.loc[len(df_new)] = series_temp
                        print("campionamento finestra passiva")
                    else:
                        #finestra n+1 attiva
                        current_window += 1
                        count_fin_attiva += 1
                        series_temp = pd.concat([pd.Series({'Scale': 1}), df_timestamps.loc[index]])
                        df_new.loc[len(df_new)] = series_temp
                        print("campionamento finestra attiva")
                else:
                    #ultima finestra passiva
                    count_fin_passiva += 1
                    series_temp = pd.concat([pd.Series({'Scale': 0}), df_timestamps.loc[index]])
                    df_new.loc[len(df_new)] = series_temp
                    print("campionamento finestra passiva")
        if mean==False:
            df_new.to_csv(Path(save_dir,f"scale_"+file),index=False)

        print("\ncount eliminate: " + str(count_eliminate))
        print("count finestra attiva: " + str(count_fin_attiva))
        print("count finestra passiva: " + str(count_fin_passiva))
        print("current window: " + str(current_window))
        print("fine file")

print("fine")
