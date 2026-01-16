import numpy as np
import matplotlib.pyplot as plt
import torch
from scipy.interpolate import interp1d
import pandas as pd
import os


def load_csv_analysis(dataset_id, sep_idxs, threshold):
    df = pd.read_csv(f'EL/{dataset_id}/heavy_{sep_idxs}.csv')
    
    count_nums = df['count'].values
    count_nums = count_nums[count_nums > threshold]
    count_nums.sort()
    # 统计频数
    value_counts = pd.Series(count_nums).value_counts().sort_index()
    values = value_counts.index.to_numpy()
    frequencies = value_counts.values
    flited_val_freq = np.concatenate((values, frequencies), axis=0).reshape(2,-1).T
    return flited_val_freq

def main(comb_name, model_name, train_num, value1, value2):
    zuhe_ls = [f'{i}' for i in range(11)]
    if comb_name == 1:
        train_ls, test_ls = zuhe_ls[:train_num], zuhe_ls[train_num:]
        root = 'comb_1'
        
    preds1 = np.load(f'{root}/{model_name}_1_10_results_1e-2/preds_{value1}.npy')     
    preds2 = np.load(f'{root}/{model_name}_10_1e4_results_1e-2/preds_{value2}.npy')
    preds = np.column_stack((preds1, preds2))
    def make_dir(dir_name):
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            
    def get_closest_to_avg_preds(preds):
        closest_wmrd_ls = []       # 存储最接近平均值的WMRD值
        closest_wmrd_pred_ls = []   # 存储对应的全局索引（即id）
        mrd_avg_ls = []            # 存储每个数据集的平均MRD
        wmrd_avg_ls = []           # 存储每个数据集的平均WMRD

        for idx, dataset_id in enumerate(test_ls):
            print('='*80)
            print(dataset_id)
            val_wmrd_ls = []
            val_mrd_ls = []
            interp_ls = []

            # 加载真实值数据
            real_trues_1 = np.load(f'tr_ts/1_10_real/{dataset_id}.npy')
            real_trues_2 = np.load(f'tr_ts/10_1e4_real/{dataset_id}.npy')
            real_trues = np.vstack((real_trues_1, real_trues_2))
            
            # 获取当前数据集对应的预测值
            corr_preds = preds[idx*100 : (idx+1)*100]
            
            # 遍历每个预测样本并计算指标
            for sep_idx, sep_preds in enumerate(corr_preds):
                flited_val_freq = load_csv_analysis(dataset_id, 0, 1000)
                # 频率插值逻辑
                target_freqs = np.concatenate((
                    np.arange(1, 1001, 1),
                    np.arange(1001, 10001, 10)
                ))
                
                sep_preds[sep_preds < 0] = 0  # 处理负值
                interp_func = interp1d(target_freqs, sep_preds, 
                                    kind='linear', fill_value='extrapolate')
                new_target_freqs = np.arange(1, 1001, 1)
                interpolated_preds = interp_func(new_target_freqs)
                                
                
                filted_tgt_freqs = flited_val_freq[:,0]
                filted_sep_preds = flited_val_freq[:,1]
                filted_interp_func = interp1d(filted_tgt_freqs, filted_sep_preds,
                                              kind='linear', fill_value='extrapolate')
                filted_new_target_freqs = np.arange(1001, 20001, 1)
                filted_interpolated_preds = filted_interp_func(filted_new_target_freqs)
                
                cat_interpolated_preds = np.concatenate((interpolated_preds, filted_interpolated_preds))

                # 最终评估
                final_inter_preds = cat_interpolated_preds[real_trues[:,0]-1]
                final_inter_preds = np.around(final_inter_preds, 0)
                final_inter_preds[final_inter_preds < 0] = 0  # 处理负值
                interp_ls.append(final_inter_preds)
                
                # 计算MRD和WMRD
                val_mrd = np.around(
                    np.mean(np.abs(final_inter_preds - real_trues[:,1]) / 
                        ((final_inter_preds + real_trues[:,1])/2)), 
                    6
                )
                val_wmrd = np.around(
                    np.mean(np.abs(final_inter_preds - real_trues[:,1])) / 
                    np.mean((final_inter_preds + real_trues[:,1])/2), 
                    6
                )
                val_mrd_ls.append(val_mrd)
                val_wmrd_ls.append(val_wmrd)
            
            # 计算当前数据集的平均指标
            mrd_avg = np.mean(val_mrd_ls)
            wmrd_avg = np.mean(val_wmrd_ls)
            mrd_avg_ls.append(mrd_avg)
            wmrd_avg_ls.append(wmrd_avg)
            
            # 找到最接近平均WMRD的样本索引
            val_wmrd_arr = np.array(val_wmrd_ls)
            differences = np.abs(val_wmrd_arr - wmrd_avg)
            closest_idx = np.argmin(differences)  # 找到最小差异的索引
            
            # 记录结果
            closest_wmrd_ls.append(val_wmrd_arr[closest_idx])
            closest_wmrd_pred_ls.append(interp_ls[closest_idx])  # 全局索引
            
            print(len(interp_ls))
            print(len(interp_ls[closest_idx]))
            
        return mrd_avg_ls, wmrd_avg_ls, closest_wmrd_ls, closest_wmrd_pred_ls
    
    mrd_avg_ls, wmrd_avg_ls, closest_wmrd_ls, closest_wmrd_pred_ls = get_closest_to_avg_preds(preds)
    
    mrd_avg_ls.append(np.mean(mrd_avg_ls))
    wmrd_avg_ls.append(np.mean(wmrd_avg_ls))
    
    df = pd.DataFrame({
        'dataset_id': test_ls+['avg'],
        'mrd_avg': mrd_avg_ls,
       'wmrd_avg': wmrd_avg_ls
    })
    df.to_csv(f'comb_{model_name}_{comb_name}.csv', index=False)
    print(f'mrd_avg: {np.mean(np.array(mrd_avg_ls))}')
    print(f'wmrd_avg: {np.mean(np.array(wmrd_avg_ls))}')
    print(f'worse_avg_wmrd: {np.mean(np.array(closest_wmrd_ls))}')
    print(len(closest_wmrd_pred_ls))
    
    def plot_sep_worse(preds, closest_wmrd_pred_ls):
        for idx, dataset_id in enumerate(test_ls):
            print('='*80)
            print(f'plotting dataset_id: {dataset_id}')
            real_trues_1 = np.load(f'tr_ts/1_10_real/{dataset_id}.npy')
            real_trues_2 = np.load(f'tr_ts/10_1e4_real/{dataset_id}.npy')
            real_trues = np.vstack((real_trues_1, real_trues_2))
            
            final_inter_preds = closest_wmrd_pred_ls[idx]
            
            val_mrd = np.around(np.mean(np.abs(final_inter_preds - real_trues[:,1]) / 
                                    ((final_inter_preds + real_trues[:,1])/2)), 6)
            val_wmrd = np.around(np.mean(np.abs(final_inter_preds - real_trues[:,1])) / 
                            np.mean((final_inter_preds + real_trues[:,1])/2), 6)
            
            save_path = f'plots/comb_{comb_name}_{model_name}'
            make_dir(save_path)
            # Visualization
            plt.figure(figsize=(10, 7))
            plt.plot(np.log10(real_trues[:,0]), np.log10(real_trues[:,1]), label='True')
            
            drop_index = np.where(final_inter_preds == 0)
            adjusted_preds = np.delete(final_inter_preds, drop_index)
            adjusted_trues = np.delete(real_trues[:,0], drop_index) 
                    
            plt.plot(np.log10(adjusted_trues), np.log10(adjusted_preds),  label='Predicted')
            
            plt.xlabel('log10(Flow Size)')
            plt.xlim(-0.05, 5)
            plt.ylabel('log10(Frequency)')
            plt.title(f'MRD: {val_mrd}, WMRD: {val_wmrd}')
            plt.legend()
            plt.savefig(f'{save_path}/{dataset_id}.png', dpi=600)
            plt.close()

            print(f'Comb {comb_name}, Dataset {dataset_id}, MRD: {val_mrd}, WMRD: {val_wmrd}')
            
    plot_sep_worse(preds, closest_wmrd_pred_ls)

if __name__ == "__main__":
    comb_name = 1
    model_name = 'ViT'
    train_num = 2
    if comb_name == 1:
        value1 = 'preds_0.037098'
        value2 = 'preds_0.093029'
        value1 = value1.replace('preds_', '')
        value2 = value2.replace('preds_', '')
        main(comb_name, model_name, train_num, value1, value2)