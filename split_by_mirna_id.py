"""
Author:     LanHao
Date:       2020/11/25
Python:     python3.6

根据miRNA id 将汇总后的数据进行分组

"""

import os
import logging

logging.basicConfig(level=logging.DEBUG)

#  准备存放分离后的数据
split_path = os.path.join(os.getcwd(), "split_mirnaid_2")
if not os.path.exists(split_path):
    os.mkdir(split_path)


def get_mirna_id_family(mirnaid: str) -> str:
    """
    从mirnaid 整理得到基因所在家庭,xx-xx-xx-xx 提取前三
    :param mirnaid:
    :return:
    """
    mirnaid = mirnaid.strip()
    mirnaids = mirnaid.split("-")
    if len(mirnaids) < 3:
        raise Exception(f"基因ID 无法找到基因家庭:{mirnaid}")
    family = "-".join(mirnaids[0:3])
    return family


# data_file = "TCGA-miR1.csv"
data_file = "TCGA-miR.csv"
data_cache = {}

with open(data_file, "r") as f:
    header = f.readline() # 跳过表头
    while True:
        data = f.readline()
        if not data:
            break
        columns = data.split(",")
        mirnaid = columns[1] # 用于分割的第几列,第2列是基因ID
        mirnaid = mirnaid[1:-1] # 去除基因ID 的引号
        mirna_family = get_mirna_id_family(mirnaid)
        if mirna_family not in data_cache.keys():
            data_cache[mirna_family] = []
        data_cache[mirna_family].append(data)

logging.debug(f"加载完毕")
for key,values in data_cache.items():
    file_save_name = os.path.join(split_path,f"{key}.csv")
    with open(file_save_name,"w+") as fw:
        fw.write(header)
        fw.writelines(values)
