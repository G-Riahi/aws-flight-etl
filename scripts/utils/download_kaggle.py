import kagglehub
import os
import shutil

target_folder = os.path.join(os.getcwd(), "data", "raw")

path = kagglehub.dataset_download("usdot/flight-delays")

for item in os.listdir(path):
    s = os.path.join(path, item)
    d = os.path.join(target_folder, item)
    shutil.move(s, d)