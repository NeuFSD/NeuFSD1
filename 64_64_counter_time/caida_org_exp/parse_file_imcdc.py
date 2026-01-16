import os
from collections import defaultdict
from typing import DefaultDict

KEY_SIZE = 13          # 每个key固定13字节
BUFFER_SIZE = 4096     # 4KB读取缓冲区

def parse_data_file_c_style(file_path: str) -> DefaultDict[bytes, int]:
    """以13字节为单位直接读取的密钥频率统计"""
    frequency_dict = defaultdict(int)
    # 创建带余量的缓冲区（多保留12字节用于处理跨块数据）
    buffer = bytearray(BUFFER_SIZE + KEY_SIZE - 1)
    residue_size = 0  # 记录上次未处理的数据长度

    with open(file_path, 'rb') as f:
        while True:
            # 读取时预留空间拼接剩余数据
            chunk = f.read(BUFFER_SIZE - residue_size)
            if not chunk:
                break
                
            # 拼接残留数据与新数据
            chunk_start = residue_size
            chunk_end = chunk_start + len(chunk)
            buffer[chunk_start:chunk_end] = chunk
            total_size = chunk_end  # 当前缓冲区有效数据总量
            
            # 计算完整密钥段数量
            key_count = total_size // KEY_SIZE
            # 处理所有完整密钥
            for i in range(key_count):
                start_pos = i * KEY_SIZE
                key = bytes(buffer[start_pos:start_pos + KEY_SIZE])
                frequency_dict[key] += 1
            
            # 保存未完成数据到缓冲区头部
            residue_size = total_size % KEY_SIZE
            if residue_size > 0:
                buffer[:residue_size] = buffer[total_size - residue_size:total_size]

    # 处理文件末尾残留数据
    if residue_size != 0:
        print(f"警告：文件末尾发现 {residue_size} 字节不完整数据，已忽略")
        
    return frequency_dict