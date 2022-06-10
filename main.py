from asammdf import MDF
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import duckdb
import time

selected_mode = "audi"


def read_mdf_files(mode):
    sample_paths = []
    files = []
    base_path = "./sample-data"
    if mode == "truck":
        base_path = base_path + "/J1939 (truck)/LOG/958D2219/00002501/"
        sample_paths.append(base_path + "00002081.MF4")
        sample_paths.append(base_path + "00002082.MF4")
        sample_paths.append(base_path + "00002083.MF4")
        sample_paths.append(base_path + "00002084.MF4")
    if mode == "audi":
        base_path = base_path + "/OBD2 (Audi A4)/LOG/31CB1F25/00000022/"
        sample_paths.append(base_path + "00000002.MF4")
    if mode == "nissan":
        base_path = base_path + "/UDS (Nissan Leaf EV)/LOG/2F6913DB/00000020/"
        sample_paths.append(base_path + "00000001-61D9D830.MF4")
    for sample_path in sample_paths:
        files.append(MDF(sample_path))
    return files


def search_query(file_path):

    query = duckdb.query('''
    SELECT *
    FROM "''' + file_path + '''" 
    WHERE timestamps BETWEEN 0.0 AND 250.0
    ''').fetchall()

    return query


def precise_search_query(file_path):
    query = duckdb.query('''
        SELECT *
        FROM "''' + file_path + '''" 
        WHERE timestamps BETWEEN 0.01485 AND 0.01486
        ''').fetchall()

    return query


files = read_mdf_files(selected_mode)

file_index = 0
if not os.path.exists("./files/csv"):
    os.makedirs("./files/csv")
for file in files:
    file_index += 1
    group = file.get_group(0)
    file.export(fmt="csv", filename="./files/csv/example"+str(file_index))

file_names = os.listdir("./files/csv")
if not os.path.exists("./files/parquet"):
    os.makedirs("./files/parquet")
for file_name in file_names:
    df = pd.read_csv("./files/csv/" + file_name)
    df.to_parquet("./files/parquet/" + file_name + ".parquet")

file_path_parquet = "./files/parquet/example1.ChannelGroup_0.csv.parquet"
file_path_csv = "./files/csv/example1.ChannelGroup_0.csv"

total_time_csv = 0
total_time_parquet = 0
for i in range(0, 100):
    t0 = time.time_ns()
    result = search_query(file_path_csv)
    t1 = time.time_ns()
    difference = t1 - t0
    total_time_csv += difference

    t0 = time.time_ns()
    result = search_query(file_path_parquet)
    t1 = time.time_ns()
    difference = t1 - t0
    total_time_parquet += difference

avg_csv = round((total_time_csv/100)/1000000000, 4)
avg_parquet = round((total_time_parquet/100)/1000000000, 4)

print("Abfragedauer bei CSV: c.a. " + str(avg_csv) + "s")
print("Abfragedauer bei Parquet: c.a. " + str(avg_parquet) + "s")

total_time_csv = 0
total_time_parquet = 0
for i in range(0, 100):
    t0 = time.time_ns()
    result = precise_search_query(file_path_csv)
    t1 = time.time_ns()
    difference = t1 - t0
    total_time_csv += difference

    t0 = time.time_ns()
    result = precise_search_query(file_path_parquet)
    t1 = time.time_ns()
    difference = t1 - t0
    total_time_parquet += difference

avg_csv = round((total_time_csv/100)/1000000000, 6)
avg_parquet = round((total_time_parquet/100)/1000000000, 6)

print("Abfragedauer (Präzise) bei CSV: c.a. " + str(avg_csv) + "s")
print("Abfragedauer (Präzise) bei Parquet: c.a. " + str(avg_parquet) + "s")









