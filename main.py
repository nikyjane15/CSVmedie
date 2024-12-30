import pandas as pd
import os
from pathlib import Path

limits_file = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling sparcely 500/csv_results/correct.csv"
directory_timestamps_states = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling sparcely 500"
dir_results = ['results_state', 'results_cpus']
path = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/Scaling sparcely 500/No_mean_csv_results"

"""
limits_file = "Scaling with more detail/Scaling Countinus/1h/csv_results/correct.csv"
directory_timestamps_states = "Scaling with more detail/Scaling Countinus/1h"
dir_results = ['results_state', 'results_cpus']
path = "Scaling with more detail/Scaling Countinus/1h/cstate_results"
"""
mean = False
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
    temp = Path(directory_timestamps_states, dir)
    file_list = [f for f in os.listdir(temp) if
                 os.path.isfile(os.path.join(temp, f))]

    t_start = limit_timestamp[0][0]  # take the timestamp indicating the start time of the test
    t_end = limit_timestamp[-1][1]  # take the timestamp indicating the end time of the test

    # set the save directory
    save_dir = Path(path + "/" + dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    # process each file (result of a test)
    for file in file_list:
        # gets sampling timestamps
        df_timestamps = pd.read_csv(Path(temp, file))
        # Manipulation to get new columns format
        df_timestamps.rename(columns={'Unnamed: 0': 'Timestamp'}, inplace=True)
        df_columns = df_timestamps.columns.to_list()
        df_columns.insert(0, "Scale")
        df_new = pd.DataFrame(columns=df_columns)
        timestamps = df_timestamps["Timestamp"].to_list()
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

        # for to scan all samplings and subdivide them according to when they occurred (in an active window, passive window or outside the time of the test).
        for index, timest in enumerate(timestamps):
            if timest < t_start or timest > t_end:  # samplings outside the time of the test
                count_eliminate += 1
                continue
            else:  # samplings inside the time of the test
                if timest >= limit_timestamp[current_window][0] and timest <= limit_timestamp[current_window][
                    1]:  # active window n
                    # print("campionamento finestra attiva")
                    count_fin_attiva += 1  # update active count
                    # add sample features to the correct dataframe
                    series_temp = pd.concat([pd.Series({'Scale': 1}), df_timestamps.loc[index]])
                    df_new.loc[len(df_new)] = series_temp
                    temp_array_active_sample.append(df_timestamps.iloc[index])  # add the sample to the temp array
                    temp_time_stamp_array.append(timest)  # add the timestamp

                elif current_window + 1 < len(
                        limit_timestamp):  # check if there is an active window after the current one

                    if timest > limit_timestamp[current_window][1] and timest < limit_timestamp[current_window + 1][
                        0]:  # passive window n
                        # save the value for the current active window (if the array is not empty)
                        if len(temp_array_active_sample) != 0:
                            df_mean_active.append(temp_array_active_sample.copy())
                            temp_array_active_sample.clear()
                            time_stamp_active_ar.append(temp_time_stamp_array.copy())
                            temp_time_stamp_array.clear();

                        # print("campionamento finestra passiva")
                        count_fin_passiva += 1  # update passive count
                        # add sample features to the correct dataframe
                        series_temp = pd.concat([pd.Series({'Scale': 0}), df_timestamps.loc[index]])
                        df_new.loc[len(df_new)] = series_temp
                        temp_array_passive_sample.append(df_timestamps.iloc[index])  # add the sample to the temp array
                        temp_time_stamp_array.append(timest)  # add the timestamp

                    else:  # active window n+1
                        # save the value for the current passive window
                        # print("Salvo in passive -> ", temp_array_passive_sample)
                        df_mean_passive.append(temp_array_passive_sample.copy())
                        temp_array_passive_sample.clear()
                        time_stamp_passive_ar.append(temp_time_stamp_array.copy())
                        temp_time_stamp_array.clear();

                        # print("campionamento finestra attiva")
                        current_window += 1  # update current window count
                        count_fin_attiva += 1  # update active count
                        # add sample features to the correct dataframe
                        series_temp = pd.concat([pd.Series({'Scale': 1}), df_timestamps.loc[index]])
                        df_new.loc[len(df_new)] = series_temp
                        temp_array_active_sample.append(
                            df_timestamps.iloc[index, 1:])  # add the sample to the temp array
                        temp_time_stamp_array.append(timest)  # add the timestamp

                else:  # there is no active window after the current one, last passive window
                    # save the value for the current active window (if the array is not empty)
                    if len(temp_array_active_sample) != 0:
                        # print("Salvo in active -> ", temp_array_active_sample)
                        df_mean_active.append(temp_array_active_sample.copy())
                        temp_array_active_sample.clear()
                        time_stamp_active_ar.append(temp_time_stamp_array.copy())
                        temp_time_stamp_array.clear();

                    # print("campionamento ultima finestra passiva")
                    count_fin_passiva += 1
                    # add sample features to the correct dataframe
                    series_temp = pd.concat([pd.Series({'Scale': 0}), df_timestamps.loc[index]])
                    df_new.loc[len(df_new)] = series_temp
                    temp_array_passive_sample.append(df_timestamps.iloc[index, 1:])  # add the sample to the temp array
                    temp_time_stamp_array.append(timest)  # add the timestamp

        # save the value for the current passive window (the last)
        if len(temp_array_passive_sample) != 0:
            # print("Salvo in last passive -> ", temp_array_passive_sample)
            df_mean_passive.append(temp_array_passive_sample.copy())
            temp_array_passive_sample.clear()
            time_stamp_passive_ar.append(temp_time_stamp_array.copy())
            temp_time_stamp_array.clear();

        if len(temp_array_active_sample) != 0:
            # print("Salvo in last active -> ", temp_array_active_sample)
            df_mean_active.append(temp_array_active_sample.copy())
            temp_array_active_sample.clear()
            time_stamp_active_ar.append(temp_time_stamp_array.copy())
            temp_time_stamp_array.clear();

        if mean == False:
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
