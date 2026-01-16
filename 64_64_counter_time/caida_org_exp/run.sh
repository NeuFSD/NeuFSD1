cd 64_64_counter/caida_org_exp

gcc -O3 -o gen gen_ds.c

for i in {0..10}; do
    # 构建输入文件路径
    input_file="caida/${i}.dat"
    # 构建输出目录路径
    output_dir="tr_ts/input/${i}"
    mkdir -p "$output_dir"
    
    # 执行命令（修正条件判断和语法）
    if [ $i -lt 2 ]; then
        ./gen --input "$input_file" --output-dir "$output_dir" --start-seed 0 --end-seed 1000
    else 
        ./gen --input "$input_file" --output-dir "$output_dir" --start-seed 0 --end-seed 100
    fi
done

python imcdc_10.py
python imcdc_1e4.py

COMB_ID=1
TRAIN_NUM=2

python imcdc_train_test_10_vit.py --comb_id ${COMB_ID} --train_num ${TRAIN_NUM}
python imcdc_train_test_1e4_vit.py --comb_id ${COMB_ID} --train_num ${TRAIN_NUM}

