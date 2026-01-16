from collections import defaultdict
import numpy as np
from scipy.interpolate import interp1d
from parse_file_imcdc import parse_data_file_c_style
import os
import pandas as pd

def make_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def process_dat_file(file_name, i, real_dir, chazhi_dir):
    # 统计每个 key 的出现次数
    frequency_dict = parse_data_file_c_style(file_name)
    # 统计每个频率对应的 key 的数量
    frequency_count = defaultdict(int)
    for count in frequency_dict.values():
        frequency_count[count] += 1

    # 按纵坐标（频率）从小到大排序
    sorted_data = sorted([(freq, num_keys) for freq, num_keys in frequency_count.items()], key=lambda x: x[0])
    real_data = [(item[0], item[1]) for item in sorted_data if item[0] > 10 and item[0] <= 50000]
    real_data = np.array(real_data)
    
    if i == 0:
        df = pd.DataFrame(real_data, columns=['x', 'y'])
        df.to_csv(real_dir + str(i) + '.csv', index=False)

    # for item in real_data:
    #     print(f'{item[0]} {item[1]}')
    
    # 假设 sorted_data 是按频率排序后的数据，格式为 [(频率, num_keys), ...]
    # 分解x（频率）和y（num_keys）
    x = np.array([item[0] for item in sorted_data])
    y = np.array([item[1] for item in sorted_data])

    # 创建插值函数，允许外推
    interp_func = interp1d(x, y, kind='linear', fill_value='extrapolate')

    target_freq_1 = np.arange(11, 1001, 1)
    
    target_freq_2 = np.arange(1001, 10001, 10)
    
    target_freqs = np.concatenate((target_freq_1, target_freq_2))

    # 计算对应的num_keys
    interpolated_values = interp_func(target_freqs)

    # 结果组合为元组列表，格式 [(频率, 插值后的num_keys), ...]
    result = list(zip(target_freqs, interpolated_values))
    final_result = np.array([key_num for freq_num, key_num in result])
    print(real_data.shape)
    print(final_result.shape)
    
    
    np.save(f'{real_dir}/{i}.npy', real_data)
    np.save(f'{chazhi_dir}/{i}.npy', final_result)
    

real_dir = 'tr_ts/10_1e4_real'
chazhi_dir = 'tr_ts/10_1e4_chazhi'
make_dir(real_dir)
make_dir(chazhi_dir)
for i in range(11):
    sep_file_name = f'caida/{i}.dat'
    process_dat_file(sep_file_name, i, real_dir, chazhi_dir)