"""
Author:     LanHao
Date:       2020/11/24
Python:     python3.6

协助TQ 处理 TCGA 数据

"""
import os
import copy
import json
import logging
import configparser
from typing import List, Dict, Union

logging.basicConfig(level=logging.DEBUG,filename="log.log")

cf = configparser.ConfigParser()
cf.read(os.path.join(os.getcwd(), "combine.ini"))

gdc_download: List[str] = []
try:
    gdc_download_mid = cf.get("GDC", "gdc_download")
    gdc_download_mid = gdc_download_mid.strip()
    gdc_download = gdc_download_mid.split(",")
except Exception as e:
    logging.error(f"{e}")

metadata = cf.get("GDC", "metadata")
assert os.path.exists(metadata), Exception(f"metadata 文件{metadata}不存在")

combinefile = cf.get("GDC", "combinefile")

datacolumns = []
try:
    datacolumn_mid = cf.get("GDC", "datacolumn")
    datacolumn_mid = datacolumn_mid.strip()
    datacolumns = datacolumn_mid.split(",")
except Exception as e:
    logging.error(f"{e}")

filecolumns = []

try:
    filecolumns_mid = cf.get("GDC", "filecolumn")
    filecolumns_mid = filecolumns_mid.strip()
    filecolumns = filecolumns_mid.split(",")
except Exception as e:
    logging.error(f"{e}")


def load_files(path: str) -> List[Dict]:
    """
    载入metadata.cart json 文件
    :param path:
    :return:
    """
    assert os.path.exists(path), "文件不存在"
    with open(path, "r") as f:
        data = json.load(f)
    return data


def load_manifest(paths: List[str]) -> Dict:
    """
    通过文件下载的MAINFSET.txt 文件,返回一个字典,key-value对应
    :param paths:
    :return:
    """
    data_cache = {}

    for path in paths:
        try:
            assert os.path.exists(path), Exception(f"文件{path} 不存在")
            base_dir = os.path.dirname(path)
            with open(path, "r") as fr:
                fr.readline()  # 跳过第一行
                while True:
                    data_row = fr.readline()
                    if not data_row:
                        break

                    data_row = data_row.strip()
                    row_splited = data_row.split("	")
                    file_id = row_splited[0]
                    file_path = row_splited[1]
                    data_cache[file_id] = os.path.join(base_dir, file_path)
                    if not os.path.exists(data_cache[file_id]):
                        logging.debug(f"数据文件{data_cache[file_id]}不存在")
        except Exception as e:
            logging.error(f"{e}")
        else:
            logging.debug(f"处理完文件:{path}")
    return data_cache


def get_file_headers(file_config: Union[Dict, List], attrn: str):
    """
    返回一个属性,表示从中提取属性值,用递归完成
    :param file_config:
    :param attrn:
    :return:
    """
    file_config = copy.deepcopy(file_config)
    data_back = "N"
    if isinstance(file_config, list):
        assert len(file_config)==1,Exception("程序暂不支持对多行列表中的属性取值")
        data_back = get_file_headers(file_config[0], attrn)
    elif isinstance(file_config, dict):
        values = attrn.split(".")
        while values:
            value = values[0].strip()
            data_mid = file_config.get(value)
            if isinstance(data_mid, list):
                data_back = get_file_headers(data_mid, ".".join(values[1:]))
                break
            elif isinstance(data_mid, dict):
                file_config = data_mid
                values = values[1:]
                continue
            else:
                data_back = data_mid
                break

    return data_back  # 返回数据副本


def get_need_columns_in_data(headers_use:Dict,columns:List,datacolumns:List):
    """
    提取指定列数据
    :param headers_use:
    :param columns:
    :return:
    """
    data_row_data = []
    for datacolumns_tmp in datacolumns:
        index = headers_use.get(datacolumns_tmp)

        if index is None:
            logging.warning(f"数据表中不存在带提取的行信息:{datacolumns_tmp}")
            data_row_data.append("N")
        else:
            if index < len(columns):
                data_row_data.append(columns[index])
            else:
                logging.debug(f"超出索引限制,请检查数据格式,len:{len(columns)},index:{index}")
                data_row_data.append("N")
    return data_row_data

def combine_data_files(file_keys: Dict, files_json: List[Dict], path: str, datacolumns, filecolumns) -> None:
    """
    合并数据到指定文件
    :param file_keys:
    :param files_json:
    :param path:
    :return:
    """
    first_row = "	".join(filecolumns)+"	"+"	".join(datacolumns)
    count = 0

    with open(path, "w+") as fw:
        fw.write(f"{first_row}\n")
        for file_need_handle in files_json:
            file_need_handle: Dict
            data_row_file = []
            for file_config_column in filecolumns:
                data_row_file.append(get_file_headers(file_need_handle,file_config_column))
            file_id = file_need_handle.get("file_id")
            if not file_id:
                logging.warning(f"文件{file_need_handle}未找到文件ID,将跳过处理")
                continue
            file_path = file_keys.get(file_id)
            if not file_path:
                logging.warning(f"下载文件中未找到所需文件:{file_need_handle}")
                continue

            with open(file_path, "r") as f:
                headers_tmp = f.readline()  # 跳过第一行

                headers_tmp = headers_tmp.strip()
                headers_tmp_list = headers_tmp.split("	")
                headers_use = dict(zip(headers_tmp_list,[i for i in range(len(headers_tmp_list))]))

                while True:
                    data = f.readline()
                    if not data:
                        break
                    data = data.strip()
                    columns = data.split("	")
                    data_row_data = get_need_columns_in_data(headers_use,columns,datacolumns)
                    fw.write(f"{'	'.join(data_row_file)}	{'	'.join(data_row_data)}\n")
                    count += 1
                logging.debug(f"结束文件:{file_path} 处理")

    logging.debug(f"累计处理数据条数:{count}")


json_files = load_files(metadata)

gdc_download_files = [os.path.join(path, "MANIFEST.txt") for path in gdc_download]

file_keys = load_manifest(gdc_download_files)

combine_data_files(file_keys, json_files, combinefile,datacolumns,filecolumns)
logging.debug(f"总共处理文件数:{len(json_files)}")
input("按enter 键结束")
