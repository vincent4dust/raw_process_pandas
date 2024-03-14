import os, json
import sys
import datetime
import pandas as pd

# file_name = sys.argv[1]
file_name = "/Users/vincentng/Documents/code/python/ADB Raw Data Process/raw.txt"

current_dir = os.getcwd()
datestr = datetime.datetime.now()
out_filename = f"result_{datestr}.csv"
out_file = os.path.join(current_dir, out_filename)
print(out_file)
# print(current_dir)

with open(file_name, "r") as f:
    lines = f.readlines()

data = []
# job_id, "creator_user_name", "name", "notebook_path", "tags", "created_time"
for line in lines:
    # print(line)
    job_id_head_index = line.find("job_id:") + 7
    job_id_trail_index = line.find(",") + 1
    job_id = line[job_id_head_index:job_id_trail_index]
    creator_head_index = line.find("creator_user_name") + 19
    creator_tail_index = line.find("setting") - 2
    creator = line[creator_head_index:creator_tail_index].replace("\"", "")
    name_head_index = line.find("\"name\"") + 7
    name_tail_index = line.find("new_cluster") - 2
    name = line[name_head_index:name_tail_index].replace("\"", "")
    notebook_head_index = line.find("notebook_path") + 15
    notebook_tail_index = line.find("\"source\"") - 2
    notebook = line[notebook_head_index:notebook_tail_index].replace("\"", "")
    tag_head_index = line.find("\"tags\"") + 7
    tag_tail_index = line.find("\"format\"") - 1
    tag = line[tag_head_index:tag_tail_index]
    time_head_index = line.find("created_time") + 14
    time_tail_index = line.rfind("}")
    time = line[time_head_index:time_tail_index]
    time = float(time)/1000
    date_time = datetime.datetime.fromtimestamp(time)
    tmp = [job_id, creator, name, notebook, tag, date_time]
    data.append(tmp)
    # print(date_time)
    # print(line[time_head_index:time_tail_index])

columns = ["job_id", "creator_user_name", "name", "notebook_path", "tags", "created_time"]
df = pd.DataFrame(data=data, columns=columns)
# df.to_csv(out_file, index=False)