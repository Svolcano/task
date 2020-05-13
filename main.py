# python 3.8.4
# done with vscode
# 2020-05-12
# 19672952it [03:04, 106889.76it/s]
# 807868.858862272 2015-12

import os
import csv
import datetime
from tqdm import tqdm
import json
from copy import deepcopy
import matplotlib.pyplot as plt

cur_dir = os.path.dirname(os.path.realpath(__file__))

tmp_file = os.path.join(cur_dir, 'out.json')

def get_src(file_name='btctradeCNY.csv'):
    return os.path.join(cur_dir, file_name)


def gen_date_from_ts(timestamp):
    date_obj = datetime.datetime.fromtimestamp(timestamp)
    date_str = date_obj.strftime("%Y-%m-%d %H:%M:%S")
    year_month_key = date_str[:7]
    return date_str, year_month_key


def wash_data(file_path):
    if os.path.exists(tmp_file):
        with open(tmp_file, 'r') as rh:
            return json.load(rh)
    line_set = set()  # remove duplicate data
    with open(file_path, 'r') as rh:
        csv_reader = csv.reader(rh)
        formated_data = {}  # {year_month_key:[line, line]}
        for line in tqdm(csv_reader):
            ts, price, volume = line
            line_key = ','.join(line)
            # ts, price and volume are equal ,then it should be deleted.
            if line_key in line_set:
                continue
            line_set.add(line_key)
            ts = int(ts)
            price = float(price)
            volume = float(volume)
            ds, key = gen_date_from_ts(ts)
            v = (ds, price, volume)
            if key in formated_data:
                formated_data[key]['data'].append(v)
                formated_data[key]['monthly_volume'] += volume
            else:
                formated_data[key] = {
                    'data': [v],
                    'monthly_volume': volume,
                }
    del line_set
    with open(tmp_file, 'w') as wh:
        json.dump(formated_data, wh)
    return formated_data


def plot(data):
    x = []
    y = []
    pick_set = set()
    # only plot year-month-day
    for one in data:
        ts, price, _ = one
        period_ts = ts[:10]
        if period_ts in pick_set:
            continue
        pick_set.add(period_ts)
        x.append(period_ts)
        y.append(price)
    del pick_set
    print(len(x), len(y))
    plt.figure(figsize=(8,6))
    plt.xlabel('date')
    plt.xticks(rotation=270, fontsize=8)
    plt.ylabel('price')
    plt.plot(x, y)
    plt.subplots_adjust(bottom=0.24)
    plt.autoscale(tight=True)
    plt.show()



def process():
    file_path = get_src()
    washed_data = wash_data(file_path)
    maximum_volume = 0
    maximum_key = None
    for key in tqdm(washed_data):
        monthly_volume = washed_data[key]['monthly_volume']
        if monthly_volume > maximum_volume:
            maximum_volume = monthly_volume
            maximum_key = key
            print(key, maximum_volume)
    plot_data = deepcopy(washed_data[maximum_key]['data'])
    del washed_data
    plot_data = sorted(plot_data, key=lambda x: x[0])
    print(len(plot_data))
    plot(plot_data)



if __name__ == "__main__":
    process()
