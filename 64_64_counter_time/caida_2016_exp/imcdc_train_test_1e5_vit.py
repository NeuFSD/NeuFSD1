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
from model import Two_dim_CNN, r34, CustomViT
import random
import argparse
import time

zuhe_ls = [f'{i}_{j}' for i in range(10) for j in range(10)]
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--comb_id', type=int, default='1', help='comb_id')
parser.add_argument('--train_num', type=int, default='10', help='train_num')
args = parser.parse_args()
comb_id = args.comb_id
train_num = args.train_num
print('='*80)
print(f'基本信息：comb_id={comb_id}, train_num={train_num}')

np.random.seed(42)
torch.manual_seed(42)
import torch

data_torch = []
label_torch = []

import numpy as np

def read_bin_file(filename):
    data = np.fromfile(filename, dtype='<u4') 

    if len(data) != 4096:
        print(f"⚠️ 文件长度异常: {len(data)} != 4096")
    acending_array = np.sort(data) 
    org_array = data
    decending_array = np.sort(data)[::-1]  
    final_array = np.concatenate((acending_array, org_array, decending_array))
    return final_array


def read_one_type_da(root_dir_name, dataset_id):
    folder_path = f'{root_dir_name}/input/{dataset_id}'
    files = sorted(os.listdir(folder_path), 
                key=lambda x: int(x.split('.')[0])) 

    sorted_data = []

    for file in files:
        sorted_array = read_bin_file(os.path.join(folder_path, file))
        sorted_data.append(sorted_array)

    data = np.stack(sorted_data, axis=0) 
    label_folder_path = f'{root_dir_name}/10_1e5_chazhi/{dataset_id}.npy'
    chazhi_label = np.load(label_folder_path)
    chazhi_label = chazhi_label.reshape(1, -1)
    chazhi_label = np.repeat(chazhi_label, data.shape[0], axis=0)
    data_tensor = torch.from_numpy(data).float()        
    data_tensor = torch.reshape(data_tensor, (data_tensor.shape[0], 3, 2**6, 2**6))
    label_tensor = torch.from_numpy(chazhi_label).float() 
    return data_tensor, label_tensor

if comb_id == 1:
    train_ls, test_ls = zuhe_ls[:train_num], zuhe_ls[train_num:50]
    root = 'comb_1'
if comb_id == 2:
    train_ls, test_ls = zuhe_ls[50:50+train_num], zuhe_ls[50+train_num:]
    root = 'comb_2'

for dataset_id in train_ls:
    data_tensor, label_tensor = read_one_type_da('tr_ts',dataset_id)  
    data_torch.append(data_tensor)
    label_torch.append(label_tensor)

all_data_tensor = torch.cat(data_torch, dim=0)
all_label_tensor = torch.cat(label_torch, dim=0)


print('='*80)
print('Train Dataset Info')
print(all_data_tensor.shape)
print(all_label_tensor.shape)

train_dataset = TensorDataset(all_data_tensor, all_label_tensor)


print('='*80)
print('Test Dataset Info')

test_torch = []
test_label_torch = []

for dataset_id in test_ls:
    test_data, test_label = read_one_type_da('tr_ts',dataset_id)
    test_torch.append(test_data)
    test_label_torch.append(test_label)

test_data = torch.cat(test_torch, dim=0)
test_label = torch.cat(test_label_torch, dim=0)
test_dataset = TensorDataset(test_data, test_label)
print(test_data.shape)
print(test_label.shape)

batch_size = 128
train_loader = DataLoader(
    train_dataset,
    batch_size=batch_size,
    shuffle=True,   
    num_workers=4,  
    pin_memory=True 
)

test_loader = DataLoader(
    test_dataset,
    batch_size=batch_size*32,
    shuffle=False,  
    num_workers=4,
    pin_memory=True
)

results_root_dir = f'{root}/ViT_10_1e5_results_1e-2'
if not os.path.exists(results_root_dir):
    os.makedirs(results_root_dir)

# 初始化模型
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = CustomViT(out_dim=label_tensor.shape[-1]).to(device)
# model = One_dim_CNN(in_chanel=1, input_shape=2**14, out_shape=label_tensor.shape[-1], conv1_dim=512, conv2_dim=1024).to(device)

# model = MLP(input_dim=2**14, output_dim=label_tensor.shape[-1]).to(device)
model = nn.DataParallel(model, device_ids=list(range(torch.cuda.device_count()))).to(device)

# 3. 训练配置
# --------------------------------------------------
criterion = nn.SmoothL1Loss()
optimizer = optim.Adam(model.parameters(), lr=1e-2)

# 训练记录
train_loss_history = []
val_loss_history = []
train_mae_history = []
val_mae_history = []

# 4. 训练循环 (使用tqdm)
# --------------------------------------------------
num_epochs = 2
best_wmrd = float('inf')  # 初始化为正无穷大 

time_accumulate = 0
sample_num = test_label.shape[0]

for epoch in range(num_epochs):
    # 训练阶段 (只显示loss)
    model.train()
    train_progress = tqdm(train_loader, desc=f'Epoch {epoch+1}/{num_epochs} [Train]', leave=False)
    running_loss = 0.0
    
    for inputs, targets in train_progress:
        inputs, targets = inputs.to(device), targets.to(device)
        
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, targets)
        loss.backward()
        optimizer.step()
        
        running_loss += loss.item()
        train_progress.set_postfix({'loss': f"{loss.item():.4f}"})
        
    # sch.step()
    train_loss = running_loss / len(train_loader)
    train_loss_history.append(train_loss)

    # 验证阶段 (显示所有指标)
    model.eval()
    val_progress = tqdm(test_loader, desc=f'Epoch {epoch+1}/{num_epochs} [Valid]', leave=False)

    all_preds = []
    all_targets = []
    
    with torch.no_grad():
        for inputs, targets in val_progress:
            
            time_start = time.time()

            inputs, targets = inputs.to(device), targets.to(device)
            
            outputs = model(inputs)
            
            time_end = time.time()
                        
            time_batch = time_end - time_start
            time_accumulate += time_batch / (sample_num * num_epochs)
        
print(f"🎉 Training Finished! Avg Time Cost: {time_accumulate} s")