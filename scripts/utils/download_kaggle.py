import kagglehub
import os
import shutil
from datetime import datetime

target_folder = os.path.join(os.getcwd(), "data", "raw")

path = kagglehub.dataset_download("usdot/flight-delays")
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"{now}: Datasets donloaded in another folder. Moving to {target_folder}")

for item in os.listdir(path):
    s = os.path.join(path, item)
    d = os.path.join(target_folder, item)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now}: Moving {item}")
    shutil.move(s, d)

#Deleting cached file, because if not and the script is re-executed, it will fail
cache_path = os.path.expanduser("~/.cache/kagglehub/datasets/usdot")
if os.path.exists(cache_path):
    shutil.rmtree(cache_path)