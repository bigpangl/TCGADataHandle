"""
Author:     LanHao
Date:       2020/11/27
Python:     python3.6

合并处理文件

"""
import os
from typing import List, Dict

# 提取目标ID
aims: List = None
with open("aims.txt", "r") as f:
    aims = list(filter(lambda line: line, [tmp.strip() for tmp in f.readlines()]))

tcga_cache: Dict = None
with open("TCGA_1127_use.csv", "r") as f:
    f.readline()  # 跳过表头
    lines = list(filter(lambda mid: mid, [line.strip() for line in f.readlines()]))
    tcga_cache = {line.split(',')[1]: line.split(',') for line in lines}

# 重点在于这张表的数据预处理，需要提取的数据格式应如:
#
# {
#   "rndid":[
#       "xxx",
#       "xxx",
#   ],# 后面的值,需要对应
# }
#

rna_cache: Dict = {}
tcga_ids: List = None
with open("TCGA-miR.csv", "r") as f:
    header = f.readline().strip()
    headers = header.split(',')
    tcga_ids = [tcga_id_single[1:-1].split('_')[-1] for tcga_id_single in headers[3::3]]
    while True:
        data = f.readline().strip()
        if not data:
            break
        datas = data.split(",")
        rndid = "-".join(datas[1][1:-1].split("-")[0:3])
        if rndid not in rna_cache.keys():
            rna_cache[rndid] = []
        rna_cache[rndid].append(datas[3::3])

file_path = os.path.join(os.getcwd(), "new_files")
headers_write = [
    "TCGAID",
    "TCGA_APPEND_VALUE",
    "case_submitter_id",
    "age_at_index",
    "icd_10_code",
    "figo_stage",
    "days_to_death",
    "days_to_last_follow_up",
    "vital_status"
]
column_indexs = [
    1,
    3,
    4,
    5,
    6,
    7,
    8
]  # 需要提取的数据的第几列

header_str = ",".join(headers_write)
for aim in aims:
    aim = "-".join(aim.split("-")[0:3]).lower()  # 只匹配前三个,表示匹配基因家庭,并且全部小写
    rna_file = os.path.join(file_path, f"{aim}.csv")
    with open(rna_file, "w+") as f:
        f.write(f"{header_str}\r")
        data_use = rna_cache.get(aim)
        if data_use:
            for single_rna_id_data in data_use:
                for tcga_id_index in range(len(tcga_ids)):
                    tcga_id = tcga_ids[tcga_id_index]
                    tcga_id = '-'.join(tcga_id.split('-')[0:3])
                    tcga_value = single_rna_id_data[tcga_id_index]
                    data_write = [tcga_id, tcga_value]
                    lingchuang = tcga_cache.get(tcga_id)
                    lingchuang and [data_write.append(lingchuang[index]) for index in column_indexs]
                    f.write(f"{','.join(data_write)}\r")
