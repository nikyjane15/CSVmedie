import pandas as pd
import os
from pathlib import Path
""" old path
limits_file = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling Countinus/1h/csv_results/correct.csv"
directory_timestamps_states = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling Countinus/1h"
dir_results = ['results_state', 'results_cpus']
path = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling Countinus/1h/cstate_results"
"""

limits_file = "Scaling with more detail/Scaling Countinus/1h/csv_results/correct.csv"
directory_timestamps_states = "Scaling with more detail/Scaling Countinus/1h"
dir_results = ['results_state', 'results_cpus']
path = "Scaling with more detail/Scaling Countinus/1h/cstate_results"

mean = True
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

    t_start = limit_timestamp[0][0]                 # take the timestamp indicating the start time of the test
    t_end = limit_timestamp[-1][1]                  # take the timestamp indicating the end time of the test
    
    # set the save directory
    save_dir = Path(path + "/" + dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    
    # process each file (result of a test)
    for file in file_list:
        # gets sampling timestamps
        df_timestamps = pd.read_csv(Path(temp,file))
        df_columns = df_timestamps.columns.to_list()
        df_columns.insert(0, "Scale")
        df_new = pd.DataFrame(columns=df_columns)
        timestamps = df_timestamps["Unnamed: 0"].to_list()
        # array to contain the samples that fall within the active windows. Each position in the array will indicate the corresponding active window, which will contain all sample values collected in that window
        df_mean_active = []         
        # array to contain the samples that fall within the passive windows. Each position in the array will indicate the corresponding passive window, which will contain all sample values collected in that window
        df_mean_passive = []                            
        # counter variables for sampling occurred inside active windows, passive windows or outside the range of the test
        current_window = 0
        count_fin_attiva = 0
        count_fin_passiva = 0
        count_eliminate = 0
        # temp array to contain the sample for the current window
        temp_array_active_sample = []                   # contains the sample of the current active window
        temp_array_passive_sample = []                  # contains the sample of the current passive window
        
        # for to scan all samplings and subdivide them according to when they occurred (in an active window, passive window or outside the time of the test).
        for index, timest in enumerate(timestamps):
            if timest < t_start or timest > t_end:      # samplings outside the time of the test
                count_eliminate += 1
                continue
            else:                                       # samplings inside the time of the test
                if timest >= limit_timestamp[current_window][0] and timest <= limit_timestamp[current_window][1]:           # active window n
                    #print("campionamento finestra attiva")
                    count_fin_attiva += 1               # update active count
                    # add sample features to the correct dataframe
                    series_temp = pd.concat([pd.Series({'Scale': 1}), df_timestamps.loc[index]])
                    df_new.loc[len(df_new)] = series_temp
                    temp_array_active_sample.append(df_timestamps.iloc[index,1:])   # add the sample to the temp array
                
                elif current_window + 1 < len(limit_timestamp):              # check if there is an active window after the current one                                       
                    
                    if timest > limit_timestamp[current_window][1] and timest < limit_timestamp[current_window + 1][0]:     # passive window n
                        # save the value for the current active window
                        df_mean_active.append(temp_array_active_sample)
                        temp_array_active_sample.clear()
                        
                        #print("campionamento finestra passiva")
                        count_fin_passiva += 1          # update passive count
                        # add sample features to the correct dataframe
                        series_temp = pd.concat([pd.Series({'Scale': 0}), df_timestamps.loc[index]])
                        df_new.loc[len(df_new)] = series_temp
                        temp_array_passive_sample.append(df_timestamps.iloc[index,1:])   # add the sample to the temp array
                        
                    else:                                                                                                   # active window n+1
                        # save the value for the current passive window
                        df_mean_passive.append(temp_array_passive_sample)
                        temp_array_passive_sample.clear()
                        
                        #print("campionamento finestra attiva")
                        current_window += 1             # update current window count
                        count_fin_attiva += 1           # update active count
                        # add sample features to the correct dataframe
                        series_temp = pd.concat([pd.Series({'Scale': 1}), df_timestamps.loc[index]])
                        df_new.loc[len(df_new)] = series_temp
                        temp_array_active_sample.append(df_timestamps.iloc[index,1:])   # add the sample to the temp array
                        
                else:                                               # there is no active window after the current one, last passive window
                    #print("campionamento ultima finestra passiva")
                    count_fin_passiva += 1
                    # add sample features to the correct dataframe
                    series_temp = pd.concat([pd.Series({'Scale': 0}), df_timestamps.loc[index]])
                    df_new.loc[len(df_new)] = series_temp
                    temp_array_passive_sample.append(df_timestamps.iloc[index,1:])   # add the sample to the temp array
        
        # save the value for the current passive window (the last)
        if not temp_array_passive_sample:
            df_mean_passive.append(temp_array_passive_sample)
            temp_array_passive_sample.clear()
        
        if mean == False:
            df_new.to_csv(Path(save_dir,f"scale_"+file),index=False)

        # control print
        print("\ncount eliminate: " + str(count_eliminate))
        print("count finestra attiva: " + str(count_fin_attiva) + " , dimensione di df_mean_active: " + str(len(df_mean_active)))
        print("count finestra passiva: " + str(count_fin_passiva) + " , dimensione di df_mean_passive: " + str(len(df_mean_passive)))
        print("current window: " + str(current_window))
        print("fine file")

        # calculate the average
        avg_active_window = []          # average of sampling values for each active window
        avg_passive_window = []         # average of sampling values for each passive window
        num_column = df_timestamps.iloc[index, 1:].shape[0] # take the number of the column
        sum_avg = [0] * num_column      # Crea una lista con 5 posizioni inizializzate a 0
        
        # scan all the position of the df_mean_active
        for index, active_window in enumerate(df_mean_active):
            sum_avg = [0] * num_column      # Crea una lista con 5 posizioni inizializzate a 0
            # scan all the sample saved in the current position (sample in current active windows)
            for sample in active_window:
                # sum all value of the column
                for i in range(num_column):
                    sum_avg[i] += sample.iloc[i]    # sum with the value of the i-th sample
            
            # calculate the average value for each column of this active window
            if len(active_window) != 0:
                for i in range(num_column):
                    sum_avg[i] /= len(active_window)
                
            # add the mean to the array in the index(current window) position
            avg_active_window.append(sum_avg)
        
        
        
        sum_avg = [0] * num_column      # Crea una lista con 5 posizioni inizializzate a 0
        # scan all the position of the df_mean_passive
        for index, passive_window in enumerate(df_mean_passive):
            sum_avg = [0] * num_column      # Crea una lista con 5 posizioni inizializzate a 0
            # scan all the sample saved in the current position (sample in current passive windows)
            for sample in passive_window:
                # sum all value of the column
                for i in range(num_column):
                    sum_avg[i] += sample.iloc[i]    # sum with the value of the i-th sample
            
            # calculate the average value for each column of this active window
            if len(passive_window) != 0:
                for i in range(num_column):
                    sum_avg[i] /= len(passive_window)
                
            # add the mean to the array in the index(current window) position
            avg_passive_window.append(sum_avg)
        
        
        # print the average
        print("\nActive window mean: ")
        for i in range(len(avg_active_window)):
            print("Active window " + str(i) + " ", avg_active_window[i])
            
        print("\nPassive window mean: ")
        for i in range(len(avg_passive_window)):
            print("Passive window " + str(i) + " ", avg_passive_window[i])
        
        # control exit to execute only one case and not all (debug utility)
        exit()

print("fine")