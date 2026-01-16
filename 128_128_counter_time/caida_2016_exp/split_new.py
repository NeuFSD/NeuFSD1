import os
from typing import List

PACKET_SIZE = 16
BUFFER_SIZE = 4096
KEY_OFFSET = 8

def split_raw_into_ten_groups(file_path: str) -> List[List[bytes]]:
    """直接从原始数据分割为十组，每组数据包数接近总数量的1/10"""
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
                packet = bytes(buffer[start : start + PACKET_SIZE])  # 存储完整16字节
                groups[current_group].append(packet)  # 改为保存完整包
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
    """写入完整16字节数据包"""
    for i, group in enumerate(groups):
        filename = f"caida2016_100_no_sh/{output_prefix}_{i}.dat"
        with open(filename, 'wb') as f:
            for packet in group:
                f.write(packet)  # 每条写入16字节完整数据

if __name__ == "__main__":
    for i in range(10):
        file_path = f'caida2016/formatted0{i}.dat'
        groups = split_raw_into_ten_groups(file_path)
        write_groups_to_files(groups, output_prefix=str(i))