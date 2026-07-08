#!/usr/bin/env python3
# =============================================================================
# split_raw.py <TRACE> <in_dir> <out_dir>
#   【入口第一步】把原始流量 bin 切成 1min 窗口切片 dataset_0000.dat ...
#   - in_dir 下的原始 .dat (任意数量) 按【文件名里的数值序】拼接 (2.dat 在 10.dat 之前, 不是字典序)
#   - 每个输入文件末尾不足一条 record 的残留字节会先丢弃，避免污染下一个文件的记录边界
#   - 每攒满一个 1min 窗口写一个输出文件; 末尾不足完整 1min 窗口的丢弃
#   - record_size 按 TRACE 自动取: caida_2018/_new=21, caida_2016=16, caida_org=13
#   等价于原始各 trace 的 split_new.py (已 md5 验证输出逐字节一致), 但统一入口、容忍任意文件数。
# 用法: python scripts/split_raw.py caida_org data_full/caida_org/caida data_full/caida_org/caida_1min_split
# =============================================================================
import os, sys, glob, re

RECSIZE = {'caida_2018': 21, 'caida_2018_new': 21, 'caida_2016': 16, 'caida_org': 13}
TARGET_RECORDS = 1_000_000
BUFFER_SIZE = 4 * 1024 * 1024

def numeric_key(path):
    nums = tuple(int(x) for x in re.findall(r'\d+', os.path.basename(path)))
    return (nums, os.path.basename(path))

def merge_and_split(input_files, out_dir, target_bytes, record_size):
    os.makedirs(out_dir, exist_ok=True)
    idx, cur = 0, 0
    out_path = os.path.join(out_dir, f"dataset_{idx:04d}.dat")
    out_f = open(out_path, 'wb')
    for fp in input_files:
        print(f"  + {fp}")
        size = os.path.getsize(fp)
        full_bytes = size - (size % record_size)
        if full_bytes != size:
            print(f"    [警告] {os.path.basename(fp)} 末尾 {size - full_bytes} 字节不足一条记录，已忽略")
        with open(fp, 'rb') as in_f:
            remaining = full_bytes
            while remaining > 0:
                chunk = in_f.read(min(BUFFER_SIZE, target_bytes - cur, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                out_f.write(chunk); cur += len(chunk)
                if cur == target_bytes:                 # 写满一个 1M-记录窗口
                    out_f.close(); idx += 1; cur = 0
                    out_path = os.path.join(out_dir, f"dataset_{idx:04d}.dat")
                    out_f = open(out_path, 'wb')
    out_f.close()
    if cur < target_bytes:                              # 删掉末尾不满 1min 窗口的文件
        if os.path.exists(out_path):
            os.remove(out_path)
            print(f"  [清理] 末尾窗口不足完整 1min 窗口，已删")
    return idx                                          # 完整窗口数

if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit("用法: python split_raw.py <TRACE> <in_dir> <out_dir>\n"
                 f"  TRACE ∈ {list(RECSIZE)}")
    trace, in_dir, out_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    if trace not in RECSIZE:
        sys.exit(f"❌ 未知 TRACE={trace}; 支持: {list(RECSIZE)}")
    rec = RECSIZE[trace]
    files = sorted(glob.glob(os.path.join(in_dir, '*.dat')), key=numeric_key)
    if not files:
        sys.exit(f"❌ {in_dir}/ 下没有原始 .dat")
    print(f"[split_raw] {trace} | {rec}B/记录 | {len(files)} 个原始文件(数值序): "
          f"{[os.path.basename(f) for f in files]}")
    n = merge_and_split(files, out_dir, TARGET_RECORDS * rec, rec)
    print(f"[split_raw] ✅ 生成 {n} 个完整 1min 窗口 -> {out_dir}/dataset_0000.dat ...")
