#!/bin/bash
#SBATCH --job-name=replace1
#SBATCH --ntasks=replace2
#SBATCH --partition=replace3
#SBATCH --cpus-per-task=1
#SBATCH --output=replace4
#SBATCH --exclude=scopion306

source ~/.bashrc

date

FILE=$1
outputPath="replace4"

#!/bin/bash

scopionPath="/home/aronton/tSDRG_random"
dicosPath="/ceph/work/NTHU-qubit/LYT/tSDRG_random"

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1


# 讀取 eee 檔案並解析 s1, s2, ds
while IFS=: read -r key value; do
    value=$(echo "$value" | xargs)  # 去除前後空白
    if [[ "$key" == "s1" ]]; then
        s1=$value
    elif [[ "$key" == "s2" ]]; then
        s2=$value
    elif [[ "$key" == "ds" ]]; then
        ds=$value
    elif [[ "$key" == "Spin" ]]; then
        Spin=$value
    fi
done < "$FILE"

if [[ "$Spin" =~ \. ]]; then
  Spin="${Spin//./}"
fi


if [ -d "${scopionPath}/tSDRG/Main_${Spin}" ]; then
    tSDRGpath="${scopionPath}"
    cd "${tSDRGpath}/tSDRG/Main_${Spin}"
    echo "working on scopion"

elif [ -d "${dicosPath}/tSDRG/Main_${Spin}" ]; then
    tSDRGpath="${dicosPath}"
    cd "${tSDRGpath}/tSDRG/Main_${Spin}"
    echo "working on dicos"
else
    echo "❌ 找不到 Main_${Spin} 目錄！"
    exit 1
fi
echo "📁 當前工作路徑：$(pwd)"

echo "parameterfile : $FILE"
echo "The working directory : $PWD"



# 全域變數控制是否使用 Slurm 排程、是否印出指令
use_slurm=$2
use_slurm="true"
if [ "$use_slurm" == true ]; then
    echo "use_slurm : $use_slurm"
fi
run_and_print() {
    local cmd=("$@")  # 將傳入的所有參數組成陣列

    if $use_slurm; then
        srun --ntasks=1 --nodes=1 --cpus-per-task=1 --exclusive "${cmd[@]}"
        if $print_cmd; then
            echo "[執行指令] srun --ntasks=1 --nodes=1 --cpus-per-task=1 --exclusive ${cmd[*]}"
        fi
    else
        "${cmd[@]}"
        if $print_cmd; then
            echo "[執行指令] ${cmd[*]}"
        fi
    fi
}


# 檢查是否提供了檔案名稱作為參數
if [ -z "$1" ]; then
    echo "請提供要讀取的 .txt 檔案名稱作為參數。"
    echo "用法：$0 檔案名稱.txt"
    exit 1
fi

# 檢查指定的檔案是否存在
if [ ! -f "$FILE" ]; then
    echo "檔案 '$FILE' 不存在。"
    exit 1
fi

# 逐行讀取並顯示檔案內容
task=""

while IFS= read -r line || [ -n "$line" ]; do
    IFS=':' read -r part1 part2 <<< "$line"

    echo "$line"

    if [ "$part1" == "task" ]; then
        task="$part2"
        echo "✅ 偵測到 'task'，設定 task=$task"
    fi

done < "$FILE"



# 確保變數都有值
if [[ -z "$s1" || -z "$s2" || -z "$ds" ]]; then
    echo "錯誤: s1, s2, ds 讀取失敗！"
    exit 1
fi

# 計算分組數量
cols=$(((s2 - s1 + 1) / ds ))
echo "s1: $s1, s2: $s2, ds: $ds, cols: $cols"
# 定義行數與列數
rows=$ds
# cols=$((s2/ds))
echo
echo -e "$rows"
echo -e "$cols"
# 初始化二維陣列（用一維陣列模擬）
# array=()
if [ "$task" == "submit" ]; then
    # === 一次 srun 並行（非 MPI），每輪只一個 step，所有輸出進 #SBATCH --output ===

    for ((i=0; i<cols; i++)); do
        start=$SECONDS
        echo -e "Round${i} start ${start}\n"

        start_idx=$(( s1 + i*rows ))
        end_idx=$(( start_idx + rows - 1 ))
        (( end_idx > s2 )) && end_idx=$s2
        GROUP_SIZE=$(( end_idx - start_idx + 1 ))

        # 傳遞給子任務
        export start_idx
        export FILE
        export Spin

        if [ "$Spin" = "2" ]; then
            EXE="./spin${Spin}_run.exe"
        else
            EXE="./spin_run.exe"
        fi

        export EXE

        srun --mpi=none -n "${GROUP_SIZE}" -c 1 \
            --cpu-bind=cores \
            --distribution=block:block \
            --mem-bind=local \
            bash -lc '
                p=$(( start_idx + SLURM_PROCID ))

                echo "[RUN] rank=$SLURM_PROCID p=$p executable=$EXE file=$FILE"

                exec "$EXE" "$FILE" "$p" "$p"
            '

        
        # export FILE start_idx Spin
        # if [ "$Spin" = "2" ]; then

        #     srun --mpi=none -n "${GROUP_SIZE}" -c 1 \
        #         --cpu-bind=cores \
        #         --distribution=block:block \
        #         --mem-bind=local \
        #         bash -lc '
        #             p=$(( start_idx + SLURM_PROCID ))
        #             exec ./spin2_run.exe "$FILE" "$p" "$p"
        #         '

        # else

        #     srun --mpi=none -n "${GROUP_SIZE}" -c 1 \
        #         --cpu-bind=cores \
        #         --distribution=block:block \
        #         --mem-bind=local \
        #         bash -lc '
        #             p=$(( start_idx + SLURM_PROCID ))
        #             exec ./spin_run.exe "$FILE" "$p" "$p"
        #         '

        # fi

        # srun --mpi=none -n "${GROUP_SIZE}" -c 1 --cpu-bind=cores --distribution=block:block --mem-bind=local bash -lc '
        # p=$(( start_idx + SLURM_PROCID ))
        # exec ./spin${Spin}_run.exe "$FILE" "$p" "$p"
        # '

        python "${tSDRGpath}/Subpy/combine.py" "${FILE}" "${start_idx}" "${end_idx}"
        python "${tSDRGpath}/Subpy/ave.py"     "${FILE}" "${start_idx}" "${end_idx}"
        elapsed=$(( SECONDS - start ))
        echo -e "Round${i} elapsed: $elapsed seconds\n\n $(date)\n\n"
    done

    # if $s1 != 1
    #     python "${tSDRGpath}/Subpy/combine.py" "${FILE}" 1 "${s2}"
    python "${tSDRGpath}/Subpy/ave.py" "${FILE}" 1 "${s2}"

else
    # 否則，執行這段
    # run_and_print python ${tSDRGpath}/Subpy/combine.py "${FILE}" 1 "${s2}"
    # run_and_print python ${tSDRGpath}/Subpy/ave.py "${FILE}" 1 "${s2}"

    python ${tSDRGpath}/Subpy/combine.py "${FILE}" 1 "${s2}"
    python ${tSDRGpath}/Subpy/ave.py "${FILE}" 1 "${s2}"
fi
# python /dicos_ui_home/aronton/tSDRG_random/Subpy/combine.py ${FILE}

echo "Job finished $(date)"
