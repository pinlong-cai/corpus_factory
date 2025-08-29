#!/bin/bash

# =============== 配置区域 ===============
DRY_RUN=false
MAX_PARALLEL_JOBS=50  # 最大并行任务数

# rclone remote 名称（需提前通过 rclone config 配置好）
REMOTE="s3"
BUCKET="heta"
BASE_PREFIX="raw/ccid/SEMI_TW"
TAR_PREFIX="element/ccid/semi_tw"

# 构建路径
PDF_SRC_BASE="${REMOTE}://${BUCKET}/${BASE_PREFIX}"
IMAGE_DEST="${REMOTE}://${BUCKET}/${TAR_PREFIX}/semi_tw_html/image"
PDF_DEST="${REMOTE}://${BUCKET}/${TAR_PREFIX}/semi_tw_pdf"

# ================== 辅助函数 ==================
run_cmd() {
  echo "[CMD] $*"
  if ! $DRY_RUN; then
    "$@"
  fi
}

# 处理单个图片目录的函数
process_image_dir() {
  local dir_name="$1"
  local unit="$2"
  local src_path="$3"
  local image_dest="$4"
  
  echo "[$unit:$dir_name] 开始同步..."
  
  # 执行同步
  if timeout 300 ../../rclone-v1.68.1-linux-amd64/rclone copy "$src_path" "$image_dest/" \
    --progress --transfers 200 --checkers 200 --create-empty-src-dirs=false --ignore-errors; then
    echo "[$unit:$dir_name] ✅ 同步完成"
    echo "DONE:$unit:$dir_name" >> /tmp/rclone_copy_status_$$.txt
  else
    echo "[$unit:$dir_name] ❌ 同步失败或超时"
    echo "FAIL:$unit:$dir_name" >> /tmp/rclone_copy_status_$$.txt
  fi
}

# 导出函数供并行任务使用
export -f process_image_dir
export DRY_RUN

# ================== 获取单位目录列表 ==================
echo "开始获取单位目录列表..."
echo "源路径: ${PDF_SRC_BASE}"

readarray -t unit_paths < <(
  ../../rclone-v1.68.1-linux-amd64/rclone lsf "$PDF_SRC_BASE/" \
    --dirs-only \
    --max-depth 1
)

units=()
for dir_name in "${unit_paths[@]}"; do
  dir_name="${dir_name%/}"
  if [[ -n "$dir_name" ]]; then
    units+=("$dir_name")
    echo "发现单位: $dir_name"
  fi
done

readarray -t units < <(printf '%s\n' "${units[@]}" | sort -u)

if [ ${#units[@]} -eq 0 ]; then
  echo "⚠️ 未找到任何单位目录，请检查 BASE_PREFIX 是否正确。"
  exit 1
fi

echo "📌 最终单位列表: ${units[*]}"
echo "----------------------------------------"

# ================== 预先获取所有存在的 media 目录 ==================
echo "正在预先获取所有单位的 media 目录结构..."

declare -A unit_media_dirs

for unit in "${units[@]}"; do
  media_base="${PDF_SRC_BASE}/${unit}/media"
  echo "检查单位 media 目录: $media_base"
  
  readarray -t dirs < <(
    ../../rclone-v1.68.1-linux-amd64/rclone lsf "$media_base/" --dirs-only 2>/dev/null | sed 's|/$||'
  )
  
  unit_media_dirs["$unit"]="${dirs[*]}"
  echo "  -> 存在的目录: ${dirs[*]}"
done

echo "----------------------------------------"

# ================== 并行同步图片文件 ==================
echo "📌 正在并行处理图片文件... (最大并行数: $MAX_PARALLEL_JOBS)"

# 映射：目录名 => 扩展名
declare -A image_dirs
image_dirs[bmp]="bmp"
image_dirs[gif]="gif"
image_dirs[ico]="ico"
image_dirs[jpg]="jpg jpeg"
image_dirs[jpeg]="jpg jpeg"
image_dirs[png]="png"
image_dirs[tif]="tif tiff"
image_dirs[tiff]="tif tiff"
image_dirs[webp]="webp"

# 清理之前的临时文件
rm -f /tmp/rclone_copy_status_$$.txt

# 创建任务列表
task_list=()

for unit in "${units[@]}"; do
  for dir_name in "${!image_dirs[@]}"; do
    if [[ " ${unit_media_dirs[$unit]} " =~ " $dir_name " ]]; then
      src_path="${PDF_SRC_BASE}/${unit}/media/${dir_name}/"
      task_list+=("$dir_name|$unit|$src_path")
    fi
  done
done

# 并行处理所有任务
echo "共 ${#task_list[@]} 个同步任务"

if [ ${#task_list[@]} -gt 0 ]; then
  # 使用 xargs 并行执行
  printf '%s\n' "${task_list[@]}" | xargs -I {} -P $MAX_PARALLEL_JOBS bash -c '
    IFS="|" read -r dir_name unit src_path <<< "{}"
    process_image_dir "$dir_name" "$unit" "$src_path" "'"$IMAGE_DEST"'"
  '
  
  # 或者使用 GNU parallel (如果已安装)
  # printf '%s\n' "${task_list[@]}" | parallel -j $MAX_PARALLEL_JOBS -I '{}' '
  #   IFS="|" read -r dir_name unit src_path <<< "{}"
  #   process_image_dir "$dir_name" "$unit" "$src_path" "'"$IMAGE_DEST"'"
  # '
fi

# 统计结果
if [ -f /tmp/rclone_copy_status_$$.txt ]; then
  success_count=$(grep "^DONE:" /tmp/rclone_copy_status_$$.txt | wc -l)
  fail_count=$(grep "^FAIL:" /tmp/rclone_copy_status_$$.txt | wc -l)
  
  echo "----------------------------------------"
  echo "📊 同步统计:"
  echo "   ✅ 成功: $success_count 个任务"
  echo "   ❌ 失败: $fail_count 个任务"
  
  if [ $fail_count -gt 0 ]; then
    echo "   失败的任务:"
    grep "^FAIL:" /tmp/rclone_copy_status_$$.txt | cut -d: -f2,3
  fi
fi

# 清理临时文件
rm -f /tmp/rclone_copy_status_$$.txt

echo "✅ 所有图片处理完成！"