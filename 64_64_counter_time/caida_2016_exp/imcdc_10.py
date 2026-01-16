from collections import defaultdict
import numpy as np
from scipy.interpolate import interp1d
from parse_file import parse_data_file_c_style
import os

def make_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        
def process_dat_file(file_name, i, k, real_dir, chazhi_dir):
    frequency_dict = parse_data_file_c_style(file_name)

    frequency_count = defaultdict(int)
    for count in frequency_dict.values():
        frequency_count[count] += 1

    sorted_data = sorted([(freq, num_keys) for freq, num_keys in frequency_count.items()], key=lambda x: x[0])

    real_data = [(item[0], item[1]) for item in sorted_data if item[0] <= 10]
    real_data = np.array(real_data)

    for item in real_data:
        print(f'{item[0]} {item[1]}')

    x = np.array([item[0] for item in sorted_data])
    y = np.array([item[1] for item in sorted_data])

    interp_func = interp1d(x, y, kind='linear')

    target_freqs = np.arange(1, 11, 1)

    interpolated_values = interp_func(target_freqs)

    result = list(zip(target_freqs, interpolated_values))
    final_result = np.array([key_num for freq_num, key_num in result])

    np.save(f'{real_dir}/{i}_{k}.npy', real_data)
    np.save(f'{chazhi_dir}/{i}_{k}.npy', final_result)

real_dir = 'tr_ts/1_10_real'
chazhi_dir = 'tr_ts/1_10_chazhi'
make_dir(real_dir)
make_dir(chazhi_dir)
for i in range(10):
    for k in range(10):
        sep_file_name = f'caida2016_100_no_sh/{i}_{k}.dat'
        process_dat_file(sep_file_name, i, k, real_dir, chazhi_dir)