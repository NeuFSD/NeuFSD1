from collections import defaultdict
import numpy as np
from scipy.interpolate import interp1d
from parse_file_2018 import parse_data_file_c_style
import os
import pandas as pd

def make_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        
def process_dat_file(file_name):
    # 统计每个 key 的出现次数
    frequency_dict = parse_data_file_c_style(file_name)
    # 统计每个频率对应的 key 的数量
    frequency_count = defaultdict(int)
    for count in frequency_dict.values():
        frequency_count[count] += 1
    # 按纵坐标（频率）从小到大排序
    sorted_data = sorted([(freq, num_keys) for freq, num_keys in frequency_count.items()], key=lambda x: x[0])
    real_data = [(item[0], item[1]) for item in sorted_data]
    
    if i == 0 and k == 0:
        pd.DataFrame(real_data, columns=['frequency', 'num_keys']).to_csv(f'{i}_{k}_info.csv', index=False)
    
    total_freq = sum([item[0] * item[1] for item in real_data])
    total_keys = sum([item[1] for item in real_data])
    return total_freq, total_keys

if __name__ == "__main__":
    dataset_id = [f'{i}_{k}' for i in range(10) for k in range(10)]
    freq_ls = []
    keys_ls = []
    for i in range(10):
        for k in range(10):
            print(f'processing {i}_{k}')
            sep_file_name = f'caida2018_100_no_sh/{i}_{k}.dat'
            total_freq, total_keys = process_dat_file(sep_file_name)
            freq_ls.append(total_freq)
            keys_ls.append(total_keys)
    pd.DataFrame({'dataset_id': dataset_id, 'freq': freq_ls, 'keys': keys_ls}).to_csv('stat_info.csv', index=False)
    