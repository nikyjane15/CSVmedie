import os
import re
import pandas as pd
import math
import datetime

# ------------------------------------------ Extract Timestamps --------------------------------------------- #

# Directory containing the metric files
directory = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/No scaling/metrics"
save_dir = "C:/Users/MalvaTalpa/Desktop/Scaling with more detail/No scaling"
dir_list = os.listdir(directory)

# Filter files for "Ilo"
ilo_list = [name for name in dir_list if "Ilo" in name]

# Extract timestamps from filenames
ilo_timestamps_unix = [name.split("_")[0] for name in ilo_list]
ilo_timestamps = [datetime.datetime.fromtimestamp(float(timestamp)/1000000000) for timestamp in ilo_timestamps_unix]
# timestamps needs to be in seconds! they were in nanoseconds
ilo_state = [name.split("_")[2].split(".")[0] for name in ilo_list]

# ------------------------------------------ Extract ILO Values --------------------------------------------- #

raritan_power_pattern=(r'raritanpdu_activepower_watt\{connector_id="17",label="17",pdu="raritan_rack_right",type="outlet"} (\d+\.\d+)')
raritan_energy_pattern=(r'raritanpdu_activeenergy_watthour_total\{connector_id="17",label="17",pdu="raritan_rack_right",type="outlet"} (\d+\.\d+)')

raritan_power = []
raritan_energy = []

# Extract metric values and IP addresses for Ilo files
for file in ilo_list:
    with open(os.path.join(directory, file), "r") as f:
        data = f.read()
        matches = re.findall(raritan_power_pattern, data)
        raritan_power.append(matches.pop())
        raritan_energy.append(re.findall(raritan_energy_pattern, data).pop())
# -------------------------------------------- Save CSV Data ------------------------------------------------ #

# Prepare and save Ilo data to separate CSV files for each IP address and metric detail
raritan_data_power = {
    "Timestamp": ilo_timestamps_unix,
    "Power[W]": raritan_power
}
power_df = pd.DataFrame(raritan_data_power)
power_df.to_csv(os.path.join(save_dir,f"raritan_active_power.csv",), index=False)

raritan_data_energy = {
    "Timestamp": ilo_timestamps_unix,
    "Energy[KW/h]": raritan_energy
}
energy_df = pd.DataFrame(raritan_data_energy)
energy_df.to_csv(os.path.join(save_dir,f"raritan_energy_KWH.csv",), index=False)

print("chickennn")
