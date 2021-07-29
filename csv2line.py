import glob
import os
import sys
import argparse
import pandas as pd
from tqdm import tqdm
from subprocess import call
import sys
import click

assert sys.version[:1] == "3"


@click.group()
def cli():
    """A CLI wrapper for CSV2Line"""


if __name__ == '__main__':
    cli()


def filename(path):
    return os.path.basename(path).split('.csv')[0]


g_string_fields = []
g_tag_keys = []
g_time_field = ""
g_measurement_name = ""
g_drop_fields = True

# @click.option('--output-dir', required=True, type=str, help='Output directory')


@click.option('--csv-file', required=True, type=str, help='CSVInput directory')
@click.option('--string-fields', required=False, type=str, help='Comma seperated string fields')
@click.option('--tag-keys', required=False, type=str, default="name,topic", help='Tag Key fields')
@click.option('--time-field', required=False, type=str, default="time", help='name of time field')
@click.option('--drop-fields', required=False, default='Unnamed 0', type=str, help='drop fields')
@click.option('--force', required=False, type=bool, help='force run with replace')
@click.option('--measurement', required=True, type=str, help='measurement name')
@cli.command("convert")
def cc(csv_file, string_fields, tag_keys, time_field, drop_fields, force, measurement):
    """convert csv to influx line protocol !!!"""
    global g_string_fields, g_tag_keys, g_measurement_name, g_time_field, g_drop_fields

    g_drop_fields = drop_fields.split(",")
    processing_dir = os.path.normpath(os.path.dirname(csv_file))
    output_dir = processing_dir
    g_string_fields = string_fields.split(",")
    g_tag_keys = tag_keys.split(",")
    g_measurement_name = measurement
    g_time_field = time_field

    csv_file_input = csv_file
    done_dir = "{}/.done".format(output_dir)
    file_name = os.path.basename(csv_file)
    done_flag_file = "{}/.done/{}".format(output_dir, file_name)

    if not os.path.isdir(done_dir):
        os.makedirs(done_dir, exist_ok=True)

    if not force and os.path.exists(done_flag_file):
        print("{0} exists\r\nSKIPPED!".format(done_flag_file))
        return

    # t = '{}/done.txt'.format(os.path.abspath(output_dir))

    # if os.path.exists(t):
    # 	print("{0} SKIPPED!".format(csv_dir.split("/")[-1:][0]))
    # return

    # folders = sorted(glob.glob('{0}/*.csv'.format(csv_dir)))
    # files = [{'name': filename(file)} for file in folders]
    # directory = csv_dir.split("/")[-3:]

    # db = directory[0]
    # measurement = directory[1]
    # month = directory[2]

    # p1bar = tqdm(total=len(files), unit='files')
    # write_meta(db=db, output_dir=output_dir)

    # for file in files:
    target_file = '{}/LP_{}.txt'.format(os.path.abspath(output_dir), file_name)

    df = pd.read_csv(csv_file_input)
    df = df.fillna(0)

    # measurement = df.iloc[0]['name']
    # p1bar.set_description('[{}] {}/{}'.format(file['name'].split("_")[0], "dummy-db", measurement))

    with open(target_file, "w") as out_file:
        pbar = tqdm(total=len(df), leave=False, unit='lines')
        rows = df.iterrows()
        for idx, row in rows:
            s = to_line(row)
            out_file.write(s)
            pbar.update(1)
            pbar.set_postfix(file=target_file)
        pbar.close()

    # p1bar.update(1)

    # p1bar.close()

    # t = '{}/done.txt'.format(os.path.abspath(output_dir))
    with open(done_flag_file, 'w') as meta:
        meta.write("")


def to_line(row):
    time = row[g_time_field]
    if 'host' in row:
        row = row.drop(labels=[g_time_field])
    else:
        row = row.drop(labels=[g_time_field])

    row = row[row != 0]

#     tag_keys = ['name', 'topic']
#     s = "{},topic={} ".format(name, topic)
#     for tag_key in tag_keys:
    # print(">", tag_key)

#     print(g_tag_keys)
    s = ""
    tag = "{},".format(g_measurement_name)
    for (key, val) in row.iteritems():
        # for tag_key in g_tag_keys:
        #     print(">", tag_key)
        # print("key", key)
        # if key in g_tag_keys:
        #     tag += "{}=\"{}\",".format(key, val)
        if key in g_drop_fields:
            continue
        if key in g_string_fields:
            #     val = val.replace(" ", '\\ ')
            val = val.replace(" ", '\\ ')
            tag += "{}={},".format(key, val)
        else:
            s += "{}={},".format(key, val)
    s = s[:-1] + ' ' + str(time) + '\n'
    return tag[:-1] + ' ' + s

# def write_meta(db, output_dir):
# 	lines = [
# 		"# DDL",
# 		"CREATE DATABASE {}".format(db),
# 		"",
# 		"# DML",
# 		"# CONTEXT-DATABASE: {}".format(db),
# 		""
# 	]
# 	with open(os.path.abspath(output_dir) + '/../meta.txt', 'w') as meta:
# 		meta.write("\n".join(lines))
