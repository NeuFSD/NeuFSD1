#!/bin/bash
# =============================================================================
# prepare_data.sh <TRACE>
#   从【你自己放好的真实切片】 data_full/<TRACE>/caida_1min_split/*.dat
#   一键生成 run_full.sh 需要的全部派生数据（确定性，可复现）：
#     - caida_1min_split_finetune_continue/   (fine_ 一个一个微调流, by sample_new.py)
#     - train_test_name_key.json              (滑窗 train/test 划分)
#     - tr_ts/{1_10,10_1e4}_{real,chazhi}/     (真实测试切片的标签, by imcdc)
#     - tr_ts_finetuned_continue/{...}/        (fine_ 训练切片的标签, by imcdc)
#   跑完即可:  bash scripts/run_full.sh 128_128 <TRACE>   (以及 64_64)
#
# 说明:
#   * 标签只依赖 .dat（与 counter 分辨率无关），故 data_full/<TRACE>/ 在 64/128 间共用,
#     prepare_data 只需按 TRACE 跑一次。默认用 configs/128_128_<TRACE>/ 的脚本(64≡128)。
#   * 每条 trace 的 .dat 字节格式不同，脚本会自动用该 trace 自己的 sample_new/imcdc(已适配)。
# 用法: bash scripts/prepare_data.sh caida_2018
#       PYTHON=/path/to/python bash scripts/prepare_data.sh caida_org
# =============================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; ROOT="$(cd "$HERE/.." && pwd)"
DATA_FULL_ROOT="${DATA_FULL_ROOT:-$ROOT/data_full}"
TRACE="${1:?用法: bash scripts/prepare_data.sh <TRACE>   (caida_2018|caida_2018_new|caida_2016|caida_org)}"
PY="${PYTHON:-python3}"
CFG="$ROOT/configs/128_128_${TRACE}"        # sample_new/imcdc 在 64/128 间逐字节相同
DF="$DATA_FULL_ROOT/${TRACE}"
[ -d "$CFG" ] || { echo "❌ 无配置 $CFG (TRACE 拼写?)"; exit 1; }

cd "$DF"
# 0) 若你只有【原始 bin】(放在 data_full/<TRACE>/caida/*.dat) 且尚未切分 -> 先切分成 1min 窗口
if [ -d "$DF/caida" ] && [ -n "$(ls "$DF"/caida/*.dat 2>/dev/null)" ] && [ ! -d "$DF/caida_1min_split" ]; then
  echo "--- 0/3 split_raw: 原始 bin (caida/) -> caida_1min_split/ (1min 窗口) ---"
  "$PY" "$ROOT/scripts/split_raw.py" "$TRACE" "$DF/caida" "$DF/caida_1min_split"
fi
[ -d "$DF/caida_1min_split" ] || { echo "❌ 缺数据: 请把【原始 bin】放到 $DF/caida/*.dat (会自动切分), 或把已切分切片放到 $DF/caida_1min_split/dataset_0000.dat ..."; exit 1; }
ndat=$(ls "$DF"/caida_1min_split/*.dat 2>/dev/null | wc -l)
[ "$ndat" -gt 0 ] || { echo "❌ $DF/caida_1min_split/ 里没有 .dat"; exit 1; }
echo "=== prepare_data [$TRACE] | $ndat 个 1min 窗口 | DATA_FULL_ROOT=$DATA_FULL_ROOT | CFG=$CFG ==="
# 1) fine_ 一个一个微调流 + 滑窗划分 json (sample_new 读 caida_1min_split, 写 *_finetune_continue + json)
echo "--- 1/3 sample_new: 生成 fine_ 微调流 + train_test_name_key.json ---"
PYTHONPATH="$CFG" "$PY" "$CFG/sample_new.py"

# 2) 真实测试切片的标签: imcdc(caida_1min_split) -> tr_ts/{1_10,10_1e4}_{real,chazhi}
echo "--- 2/3 imcdc: 真实测试切片标签 -> tr_ts/ ---"
PYTHONPATH="$CFG" "$PY" "$CFG/imcdc_10.py"  caida_1min_split tr_ts
PYTHONPATH="$CFG" "$PY" "$CFG/imcdc_1e4.py" caida_1min_split tr_ts

# 3) fine_ 训练切片的标签: imcdc(*_finetune_continue) -> tr_ts_finetuned_continue/{...}
echo "--- 3/3 imcdc: fine_ 训练切片标签 -> tr_ts_finetuned_continue/ ---"
PYTHONPATH="$CFG" "$PY" "$CFG/imcdc_10.py"  caida_1min_split_finetune_continue tr_ts_finetuned_continue
PYTHONPATH="$CFG" "$PY" "$CFG/imcdc_1e4.py" caida_1min_split_finetune_continue tr_ts_finetuned_continue

echo ""
echo "=== 完成 [$TRACE]。data_full/$TRACE/ 现在含: ==="
echo "  $DF/caida_1min_split/($ndat) + caida_1min_split_finetune_continue/ + train_test_name_key.json"
echo "  tr_ts/{1_10,10_1e4}_{real,chazhi}/ + tr_ts_finetuned_continue/{...}/"
date -Is > "$DF/.prepare_data_complete"
echo "→ 现在可跑:  bash scripts/run_full.sh 128_128 $TRACE   (以及 64_64 $TRACE)"
