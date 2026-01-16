import os
from typing import List

PACKET_SIZE = 21       # 每个数据包21字节
BUFFER_SIZE = 4096     # 4KB读取缓冲区
KEY_OFFSET = 13        # 密钥从第13字节开始（后8字节为密钥）
KEY_LENGTH = PACKET_SIZE - KEY_OFFSET  # 密钥长度8字节

def split_raw_into_ten_groups(file_path: str) -> List[List[bytes]]:
    """将原始数据分割为十组，每组数据包数接近总数量的1/10"""
    # 第一次遍历：计算总数据包数量
    total_packets = 0
    with open(file_path, 'rb') as f:
        packet_offset = 0
        buffer = bytearray(BUFFER_SIZE + PACKET_SIZE - 1)
        while True:
            chunk = f.read(BUFFER_SIZE - packet_offset)
            if not chunk:
                break
            total_bytes = packet_offset + len(chunk)
            total_packets += total_bytes // PACKET_SIZE
            packet_offset = total_bytes % PACKET_SIZE
            if packet_offset > 0:
                buffer[:packet_offset] = buffer[-packet_offset:]

    # 计算每组的目标数据包数
    group_size, remainder = divmod(total_packets, 10)
    group_sizes = [group_size + 1 if i < remainder else group_size for i in range(10)]

    # 第二次遍历：填充分组
    groups = [[] for _ in range(10)]
    current_group = 0
    current_count = 0
    with open(file_path, 'rb') as f:
        packet_offset = 0
        buffer = bytearray(BUFFER_SIZE + PACKET_SIZE - 1)
        while True:
            chunk = f.read(BUFFER_SIZE - packet_offset)
            if not chunk:
                break
            buffer[packet_offset:packet_offset + len(chunk)] = chunk
            total_bytes = packet_offset + len(chunk)
            num_packets = total_bytes // PACKET_SIZE

            for i in range(num_packets):
                start = i * PACKET_SIZE
                end = start + PACKET_SIZE
                # 提取完整21字节数据包
                packet = bytes(buffer[start:end])
                groups[current_group].append(packet)
                current_count += 1

                # 切换分组条件
                if current_count >= group_sizes[current_group] and current_group < 9:
                    current_group += 1
                    current_count = 0

            packet_offset = total_bytes % PACKET_SIZE
            if packet_offset > 0:
                buffer[:packet_offset] = buffer[-packet_offset:]
    return groups

def write_groups_to_files(groups: List[List[bytes]], output_prefix: str = "0") -> None:
    """将分组写入文件，自动创建目录"""
    output_dir = "caida2018_100_no_sh"
    os.makedirs(output_dir, exist_ok=True)
    
    for i, group in enumerate(groups):
        filename = os.path.join(output_dir, f"{output_prefix}_{i}.dat")
        with open(filename, 'wb') as f:
            for packet in group:
                f.write(packet)
        print(f"已写入文件：{filename}")

if __name__ == "__main__":
    for i in range(10):
        file_path = f'caida2018/{i}.dat'
        if not os.path.exists(file_path):
            print(f"警告：文件 {file_path} 不存在，跳过处理")
            continue
        
        print(f"\n正在处理文件：{file_path}")
        groups = split_raw_into_ten_groups(file_path)
        write_groups_to_files(groups, output_prefix=str(i))