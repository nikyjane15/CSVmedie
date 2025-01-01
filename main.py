import pandas as pd
import os
from pathlib import Path
"""
limits_file = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling sparcely 500/csv_results/scaled.csv"
directory_timestamps_states = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling sparcely 500"
dir_results = ['results_state', 'results_cpus']
path = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling sparcely 500/No_mean_csv_results"
"""

limits_file = r"B:\Risultati Scaling\50percent cpu\New (correct) 100 pkt scaling\csv_results\scaled.csv"
end_timestamp_correct = r"B:\Risultati Scaling\50percent cpu\New (correct) 100 pkt scaling\csv_results\correct.csv"
directory_timestamps_states = r"B:\Risultati Scaling\50percent cpu\New (correct) 100 pkt scaling"
dir_results = ['results_state', 'results_cpus']
path = r"B:\Risultati Scaling\50percent cpu\New (correct) 100 pkt scaling\cstate_results"

mean = False
df_limits = pd.read_csv(limits_file)
df_limits_last = pd.read_csv(end_timestamp_correct)

a = df_limits['Sequence number'].to_list()
b = df_limits_last.iloc[len(df_limits_last.index)-1]
limit_timestamp = []
for i in range(len(a)-1):
    if i == len(a)-2:
        start = df_limits.loc[i, 'Service stop responding time (ns)']
        end = b['Receiving time (ns)']
        limit_timestamp.append((df_limits.loc[i,'Scale type'],start, end))
        break
    else:
        start = df_limits.loc[i, 'Service stop responding time (ns)']
        end = df_limits.loc[i+1, 'Service stop responding time (ns)']
        limit_timestamp.append((df_limits.loc[i,'Scale type'],start, end))

#la lunghezza di limit_timestamp e' il numero di finestre attive
# get all files in dir
for dir in dir_results:
    temp = Path(directory_timestamps_states, dir)
    file_list = [f for f in os.listdir(temp) if
                 os.path.isfile(os.path.join(temp, f))]

    t_start = limit_timestamp[0][1]  # take the timestamp indicating the start time of the test
    t_end = limit_timestamp[-1][2]  # take the timestamp indicating the end time of the test

    # set the save directory
    save_dir = Path(path + "/" + dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    # process each file (result of a test)
    for file in file_list:
        # gets sampling timestamps
        df_timestamps = pd.read_csv(Path(temp, file,), sep=';')
        # Manipulation to get new columns format
        df_columns = df_timestamps.columns.to_list()
        df_columns.insert(0, "Scale")
        df_new = pd.DataFrame(columns=df_columns)
        timestamps = df_timestamps["timestamp"].to_list()
        # array to contain the samples that fall within the active windows. Each position in the array will indicate the corresponding active window, which will contain all sample values collected in that window
        df_mean_active = []
        # array to contain the samples that fall within the passive windows. Each position in the array will indicate the corresponding passive window, which will contain all sample values collected in that window
        df_mean_passive = []
        time_stamp_active_ar = []  # array to contain the timestamp of the samples that fall within the active windows
        time_stamp_passive_ar = []  # array to contain the timestamp of the samples that fall within the passive windows
        # counter variables for sampling occurred inside active windows, passive windows or outside the range of the test
        current_window = 0
        count_fin_attiva = 0
        count_fin_passiva = 0
        count_eliminate = 0
        # temp array to contain the sample for the current window
        temp_array_active_sample = []  # contains the sample of the current active window
        temp_array_passive_sample = []  # contains the sample of the current passive window
        temp_time_stamp_array = []

        ######for index, timest in enumerate(timestamps):

        # for to scan all samplings and subdivide them according to when they occurred (in an active window, passive window or outside the time of the test).
        for index, timest in enumerate(timestamps):
            if timest < t_start or timest > t_end:  # samplings outside the time of the test
                count_eliminate += 1
                continue
            else:  # samplings inside the time of the test
                #is the timestamp inside the current window?
                if timest >= limit_timestamp[current_window][1] and timest <= limit_timestamp[current_window][2]:  # active window n
                    # print("campionamento finestra attiva")
                    if(limit_timestamp[current_window][0] == "DOWN"):
                        count_fin_passiva += 1
                        series_temp = pd.concat([pd.Series({'Scale': 0}), df_timestamps.loc[index]])
                        temp_array_passive_sample.append(df_timestamps.iloc[index])  # add the sample to the temp array
                    else:
                        count_fin_attiva += 1  # update active count
                        # add sample features to the correct dataframe
                        series_temp = pd.concat([pd.Series({'Scale': 1}), df_timestamps.loc[index]])
                        temp_array_active_sample.append(df_timestamps.iloc[index])  # add the sample to the temp array

                    temp_time_stamp_array.append(timest)
                    df_new.loc[len(df_new)] = series_temp  # add the timestamp

                elif current_window + 1 < len(limit_timestamp):  # check if there is an active window after the current one
                    #is the timestamp i am looking in the window
                        # save the value for the current active window (if the array is not empty)

                    if len(temp_array_active_sample) != 0:
                        df_mean_active.append(temp_array_active_sample.copy())
                        temp_array_active_sample.clear()
                        time_stamp_active_ar.append(temp_time_stamp_array.copy())
                        temp_time_stamp_array.clear()

                    if len(temp_array_passive_sample) != 0:
                        df_mean_passive.append(temp_array_passive_sample.copy())
                        temp_array_passive_sample.clear()
                        time_stamp_passive_ar.append(temp_time_stamp_array.copy())
                        temp_time_stamp_array.clear()

                    # print("campionamento finestra attiva")
                    current_window += 1  # update current window count
                    # add sample features to the correct dataframe
                    # add the timestamp

                else:  # there is no active window after the current one, last passive window
                    # save the value for the current active window (if the array is not empty)
                    if len(temp_array_active_sample) != 0:
                        df_mean_active.append(temp_array_active_sample.copy())
                        temp_array_active_sample.clear()
                        time_stamp_active_ar.append(temp_time_stamp_array.copy())
                        temp_time_stamp_array.clear()

                    if len(temp_array_passive_sample) != 0:
                        df_mean_passive.append(temp_array_passive_sample.copy())
                        temp_array_passive_sample.clear()
                        time_stamp_passive_ar.append(temp_time_stamp_array.copy())
                        temp_time_stamp_array.clear()

        if not mean:
            df_new.to_csv(Path(save_dir, f"scale_" + file), index=False, sep=';', decimal=',', encoding='utf-8')

        # control print
        print("\ncount eliminate: " + str(count_eliminate))
        print("count finestra attiva: " + str(count_fin_attiva) + " , dimensione di df_mean_active: " + str(
            len(df_mean_active)))
        print("count finestra passiva: " + str(count_fin_passiva) + " , dimensione di df_mean_passive: " + str(
            len(df_mean_passive)))
        print("dimensione di time_stamp_active_ar: " + str(len(time_stamp_active_ar)))
        print("dimensione di time_stamp_passive_ar: " + str(len(time_stamp_passive_ar)))
        print("current window: " + str(current_window))
        print("fine file")

        if mean==True:
        # calculate the average
            avg_active_window = []  # average of sampling values for each active window
            avg_passive_window = []  # average of sampling values for each passive window
            num_column = df_timestamps.iloc[index, 1:].shape[0]  # take the number of the column (32)
            sum_avg = [0] * (
                        num_column + 1)  # array to hold the sums and then make the average values (for each window) for each sampling characteristic

            df_temp = pd.DataFrame(columns=df_columns)
            df_total_mean = pd.DataFrame(columns=df_columns)
            counter = 0
            counter_active=0
            counter_passive=0
            while counter < len(df_mean_passive) + len(df_mean_active):
                # gets the values of the active window
                if counter_active < len(df_mean_active):
                    for index in df_mean_active[counter_active]:
                        df_temp.loc[len(df_temp)] = index
                    media = df_temp.mean()
                    media['Scale'] = 1
                    df_total_mean.loc[len(df_total_mean)] = media
                    counter_active+=1
                    counter += 1

                # gets the values of the passive window
                if counter_passive < len(df_mean_passive):
                    for index in df_mean_passive[counter_passive]:
                        df_temp.loc[len(df_temp)] = index
                    media = df_temp.mean()
                    media['Scale'] = 0
                    df_total_mean.loc[len(df_total_mean)] = media
                    counter_passive+=1
                    counter += 1

            df_round=df_total_mean.round(2)
            df_round.to_csv(Path(save_dir, f"scale_" + file), index=False, sep=';', decimal=',', encoding='utf-8')


        # control exit to execute only one case and not all (debug utility)
        # exit()

print("fine")
