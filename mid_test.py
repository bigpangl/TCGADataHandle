"""
Author:     LanHao
Date:       2020/11/26
Python:     python3.6

"""
# from lifelines import CoxPHFitter
#
# from lifelines.datasets import load_rossi
#
# rossi = load_rossi()
# cph = CoxPHFitter()
# cph.fit(rossi, duration_col='week', event_col='arrest')
# cph.plot()
# import pandas as pd
#
# df1 = pd.DataFrame({'lkey': ['foo', 'bar', 'baz','foo'],
#                     'value': [1, 2, 3,5]})
# df2 = pd.DataFrame({'rkey': ['foo', 'bar', 'baz','foo'],
#                     'value': [5, 6, 7,8]})
# df1 = df1.rename(columns={'lkey':'rkey'})
# df1 = df1.append(df2).reset_index(drop=True)
# df1.drop_duplicates(subset=["rkey","value"])
# print(df1)
#
#
# cph.fit(rossi, duration_col='week', event_col='arrest', formula="fin + wexp + age * prio")
# cph.print_summary()
import requests
import json

# fields = [
#     "file_name",
#     "cases.submitter_id",
#     "cases.samples.sample_type",
#     "cases.disease_type",
#     "cases.project.project_id"
#     ]
#
# fields = ",".join(fields)
#
# files_endpt = "https://api.gdc.cancer.gov/files"
#
# # This set of filters is nested under an 'and' operator.
# filters = {
#     "op": "and",
#     "content":[
#         {
#         "op": "in",
#         "content":{
#             "field": "cases.project.primary_site",
#             "value": ["Lung"]
#             }
#         },
#         {
#         "op": "in",
#         "content":{
#             "field": "files.experimental_strategy",
#             "value": ["RNA-Seq"]
#             }
#         },
#         {
#         "op": "in",
#         "content":{
#             "field": "files.data_format",
#             "value": ["BAM"]
#             }
#         }
#     ]
# }
#
# # A POST is used, so the filter parameters can be passed directly as a Dict object.
# params = {
#     "filters": filters,
#     "fields": fields,
#     "format": "TSV",
#     "size": "2000"
#     }
#
# # The parameters are passed to 'json' rather than 'params' in this case
# response = requests.post(files_endpt, headers = {"Content-Type": "application/json"}, json = params)
#
# print(response.content.decode("utf-8"))

import re

file_id = "b658d635-258a-4f6f-8377-767a43771fe4"

data_endpt = "https://api.gdc.cancer.gov/data/{}".format(file_id)

response = requests.get(data_endpt, headers = {"Content-Type": "application/json"})

# The file name can be found in the header within the Content-Disposition key.
response_head_cd = response.headers["Content-Disposition"]

file_name = re.findall("filename=(.+)", response_head_cd)[0]

with open(file_name, "wb") as output_file:
    output_file.write(response.content)