import numpy as np
import os
import torch
from torch.utils.data import DataLoader, TensorDataset, random_split
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score
from tqdm import tqdm
from torch.optim.lr_scheduler import CosineAnnealingLR
import csv
import os
import pickle
from model import Two_dim_CNN, r34, CustomViT, Shellow_ViT
import random
import argparse


# 设置随机种子保证可重复性
np.random.seed(42)
torch.manual_seed(42)
import torch
import numpy as np

def read_bin_file(filename):
    # 读取二进制文件，指定 dtype 为 uint32（小端序）
    data = np.fromfile(filename, dtype='<u4')  # 或者 '=u4' 表示系统原生字节序
    # 验证长度
    if len(data) != 16384:
        print(f"⚠️ 文件长度异常: {len(data)} != 16384")
    acending_array = np.sort(data)  # 升序排序
    org_array = data
    decending_array = np.sort(data)[::-1]  # 降序排序
    final_array = np.concatenate((acending_array, org_array, decending_array))
    return final_array

def read_one_type_da(root_dir_name, dataset_id):
    folder_path = f'{root_dir_name}/input/{dataset_id}'
    # 2. 获取排序后的文件列表
    files = sorted(os.listdir(folder_path), 
                key=lambda x: int(x.split('.')[0]))[:10]

    sorted_data = []

    # 3. 遍历读取并排序每个文件
    for file in files:
        sorted_array = read_bin_file(os.path.join(folder_path, file))
        sorted_data.append(sorted_array)

    # 4. 堆叠成最终数组
    data = np.stack(sorted_data, axis=0)  # 形状自动变为 [batch, point]

    data_tensor = torch.from_numpy(data).float()          # 假设输入是浮点型
    data_tensor = torch.reshape(data_tensor, (data_tensor.shape[0], 3, 2**7, 2**7))
    return data_tensor

dataset_id = '0_0'
data_tensor = read_one_type_da('tr_ts',dataset_id)  
print(data_tensor.shape)

import os
from PIL import Image
import numpy as np
import torch

# 确保目标文件夹存在
os.makedirs('data_img', exist_ok=True)

# 遍历每个样本并保存为图像
for i in range(data_tensor.shape[0]):
    # 获取第i个样本的张量，形状为(3, 128, 128)
    img_tensor = data_tensor[i]
    
    # 转换为NumPy数组
    img_np = img_tensor.numpy()
    
    # 计算全局最小值和最大值以进行归一化
    min_val = img_np.min()
    max_val = img_np.max()
    
    # 归一化处理
    if max_val != min_val:
        normalized = (img_np - min_val) / (max_val - min_val)
    else:
        normalized = np.zeros_like(img_np)
    
    # 缩放到0-255并转换为uint8类型
    img_uint8 = (normalized * 255).astype(np.uint8)
    
    # 调整维度顺序为(高度, 宽度, 通道)
    img_uint8 = img_uint8.transpose(1, 2, 0)
    
    # 创建PIL图像对象并保存
    img = Image.fromarray(img_uint8, 'RGB')
    img.save(os.path.join('data_img', f'sample_{i}.png'))

print("图像保存完成！")