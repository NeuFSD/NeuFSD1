cd 64_64_counter/caida_2018_new_exp


# 编译生成可执行文件
gcc -O3 -o gen gen_ds.c

for k in {0..9}; do
    # 循环处理每个输入文件，i从0到99
    for i in {0..9}; do
        # 构建输入文件路径（假设文件名为0_0.dat, 0_1.dat,...）
        input_file="caida2018_100_no_sh/${k}_${i}.dat"
        # 构建输出目录路径，每个i对应一个目录
        output_dir="tr_ts/input/${k}_${i}"
        mkdir -p "$output_dir"
        # 执行命令，处理当前输入文件并输出到对应目录
        ./gen --input "$input_file" --output-dir "$output_dir" --start-seed 0 --end-seed 400
    done
done

python imcdc_10.py
python imcdc_1e5.py

COMB_ID=1
TRAIN_NUM=10

python imcdc_train_test_10_vit.py --comb_id ${COMB_ID} --train_num ${TRAIN_NUM}
python imcdc_train_test_1e5_vit.py --comb_id ${COMB_ID} --train_num ${TRAIN_NUM}

COMB_ID=2
TRAIN_NUM=10

python imcdc_train_test_10_vit.py --comb_id ${COMB_ID} --train_num ${TRAIN_NUM}
python imcdc_train_test_1e5_vit.py --comb_id ${COMB_ID} --train_num ${TRAIN_NUM}