import os
import re
from pathlib import Path
import pandas as pd
import math
import datetime

# ------------------------------------------ Extract Timestamps --------------------------------------------- #


# Directory containing the metric files
path = Path(r"B:/Risultati Scaling/")
dirs = os.listdir(path)
raritan_power_pattern = (
    r'raritanpdu_activepower_watt\{connector_id="17",label="17",pdu="raritan_rack_right",type="outlet"} (\d+\.\d+)')
raritan_energy_pattern = (
    r'raritanpdu_activeenergy_watthour_total\{connector_id="17",label="17",pdu="raritan_rack_right",type="outlet"} (\d+\.\d+)')

for directory in dirs:

    new_path = Path.joinpath(path,directory)
    new_path_dirs = os.listdir(new_path)
    if directory == "base" or directory == "no scaling pod up":
        path2 = Path.joinpath(new_path,'metrics')
        save_path = new_path
        listdir=os.listdir(path2)
        ilo_list = [name for name in listdir]
        ilo_timestamps_unix = [name.split(".")[0] for name in ilo_list]
        ilo_timestamps = [datetime.datetime.fromtimestamp(float(timestamp) / 1000000000) for timestamp in
                          ilo_timestamps_unix]


        raritan_power = []
        raritan_energy = []

        # Extract metric values and IP addresses for Ilo files
        for file in ilo_list:
            with open(os.path.join(path2, file), "r") as f:
                data = f.read()
                matches = re.findall(raritan_power_pattern, data)
                raritan_power.append(matches.pop())
                raritan_energy.append(re.findall(raritan_energy_pattern, data).pop())
        # -------------------------------------------- Save CSV Data ------------------------------------------------ #

        #convert string to float
        raritan_energy_float = [round(float(item),2) for item in raritan_energy]
        raritan_power_float = [round(float(item),2) for item in raritan_power]

        # Prepare and save Ilo data to separate CSV files for each IP address and metric detail
        raritan_data_power = {
            "Timestamp": ilo_timestamps_unix,
            "Power[W]": raritan_power_float

        }
        power_df = pd.DataFrame(raritan_data_power)
        power_df.to_csv(os.path.join(save_path,f"raritan_active_power.csv",), index=False, sep=";")

        raritan_data_energy = {
            "Timestamp": ilo_timestamps_unix,
            "Energy[KW/h]": raritan_energy_float
        }
        energy_df = pd.DataFrame(raritan_data_energy)
        energy_df.to_csv(os.path.join(save_path,f"raritan_energy_KWH.csv",), index=False, sep=";")
        continue

    for pkt_scale in new_path_dirs:
        path2 = Path.joinpath(new_path,pkt_scale,'metrics')
        save_path = Path.joinpath(new_path, pkt_scale, r"results_cstate_rapl_raritan")
        listdir=os.listdir(path2)

        # Filter files for "Ilo"
        ilo_list = [name for name in listdir if "Ilo" in name]
        # Extract timestamps from filenames
        ilo_timestamps_unix = [name.split("_")[0] for name in ilo_list]
        ilo_timestamps = [datetime.datetime.fromtimestamp(float(timestamp)/1000000000) for timestamp in ilo_timestamps_unix]
        # timestamps needs to be in seconds! they were in nanoseconds
        ilo_state_temp = [name.split("_")[2].split(".")[0] for name in ilo_list]
        ilo_states = [1 if state == 'Running' else 0 if state == 'Down' else 0.5 if state in ['ScalingUp','ScalingDown'] else state for state in ilo_state_temp]

        # ------------------------------------------ Extract ILO Values --------------------------------------------- #

        raritan_power = []
        raritan_energy = []

        # Extract metric values and IP addresses for Ilo files
        for file in ilo_list:
            with open(os.path.join(path2, file), "r") as f:
                data = f.read()
                matches = re.findall(raritan_power_pattern, data)
                raritan_power.append(matches.pop())
                raritan_energy.append(re.findall(raritan_energy_pattern, data).pop())
        # -------------------------------------------- Save CSV Data ------------------------------------------------ #

        # Prepare and save Ilo data to separate CSV files for each IP address and metric detail

        raritan_energy_float = [round(float(item),2) for item in raritan_energy]
        raritan_power_float = [round(float(item),2) for item in raritan_power]

        print(raritan_power_float)

        raritan_data_power = {
            "Timestamp": ilo_timestamps_unix,
            "Status": ilo_states,
            "Power[W]": raritan_power_float

        }
        power_df = pd.DataFrame(raritan_data_power)
        power_df.to_csv(os.path.join(save_path,f"raritan_active_power.csv",), index=False, sep=";", encoding="utf-8")

        raritan_data_energy = {
            "Timestamp": ilo_timestamps_unix,
            "Status": ilo_states,
            "Energy[KW/h]": raritan_energy_float
        }
        energy_df = pd.DataFrame(raritan_data_energy)
        energy_df.to_csv(os.path.join(save_path,f"raritan_energy_KWH.csv",), index=False, sep=";", encoding="utf-8")

        print("chickennn")
