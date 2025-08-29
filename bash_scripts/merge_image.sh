#!/bin/bash

# =============== 配置区域 ===============
DRY_RUN=false
MAX_PARALLEL_JOBS=20  # 最大并行任务数

# rclone remote 名称（需提前通过 rclone config 配置好）
REMOTE="s3"
BUCKET="heta"
BASE_PREFIX="test/element/30/ecnu/en_web_nih/nih_html"
MEDIA_SRC="${REMOTE}://${BUCKET}/${BASE_PREFIX}/media"
IMAGE_DEST="${REMOTE}://${BUCKET}/${BASE_PREFIX}/image"

# ================== 辅助函数 ==================
run_cmd() {
  echo "[CMD] $*"
  if ! $DRY_RUN; then
    "$@"
  fi
}

# 处理单个图片文件的函数
copy_single_image() {
  local filename="$1"
  local mapping_file="$2"
  
  # 从预处理的映射文件中查找
  mapping_line=$(grep "^${filename}|" "$mapping_file")
  full_path=$(echo "$mapping_line" | cut -d'|' -f2)
  
  if [ "$full_path" != "NOT_FOUND" ] && [ -n "$full_path" ]; then
    src_full_path="${MEDIA_SRC}/${full_path}"
    dest_full_path="${IMAGE_DEST}/${filename}"
    
    # 直接在S3中复制文件
    if ../../rclone-v1.68.1-linux-amd64/rclone copyto "$src_full_path" "$dest_full_path" --ignore-errors; then
      echo "SUCCESS:$filename"
    else
      echo "FAIL:$filename"
    fi
  else
    echo "NOT_FOUND:$filename"
  fi
}

# 导出函数供并行任务使用
export -f copy_single_image
export DRY_RUN
export MEDIA_SRC
export IMAGE_DEST

# ================== 主处理逻辑 ==================
echo "开始处理图片文件..."
echo "源路径: $MEDIA_SRC"
echo "目标路径: $IMAGE_DEST"

# 清理之前的临时文件
rm -f /tmp/rclone_*.txt.$$

# 创建目标目录
echo "创建目标目录..."
run_cmd ../../rclone-v1.68.1-linux-amd64/rclone mkdir "$IMAGE_DEST"

# 获取需要处理的文件列表
echo "读取需要处理的文件列表..."
if [ ! -f "nih_image_hash.txt" ]; then
  echo "❌ 找不到 nih_image_hash.txt 文件"
  exit 1
fi

# 计算总文件数
total_files=$(wc -l < nih_image_hash.txt)
echo "总共需要处理 $total_files 个文件"

# ================== Python预处理阶段 ==================
echo "开始预处理文件映射..."

# 创建Python预处理脚本
cat > /tmp/preprocess_$$.py << 'EOF'
import sys

# 读取所有文件路径并建立映射
all_files = {}
for line in sys.stdin:
    line = line.strip()
    if line:
        # 处理路径分隔符，确保兼容不同系统
        parts = line.split('/')
        if parts:
            filename = parts[-1]
            # 处理同名文件情况，保存第一个找到的
            if filename not in all_files:
                all_files[filename] = line

# 读取需要的文件列表
try:
    with open('nih_image_hash.txt', 'r') as f:
        needed_files = [line.strip() for line in f if line.strip()]
except FileNotFoundError:
    print("错误: 找不到 nih_image_hash.txt 文件", file=sys.stderr)
    sys.exit(1)

# 输出映射关系
for filename in needed_files:
    if filename in all_files:
        print(f"{filename}|{all_files[filename]}")
    else:
        print(f"{filename}|NOT_FOUND")
EOF

# 执行预处理：获取所有文件列表并生成映射
echo "获取媒体目录文件列表..."
../../rclone-v1.68.1-linux-amd64/rclone lsf "$MEDIA_SRC/" --recursive | \
python3 /tmp/preprocess_$$.py > /tmp/file_mapping_$$.txt

echo "预处理完成"

# 检查预处理结果
if [ ! -f "/tmp/file_mapping_$$.txt" ]; then
  echo "❌ 预处理失败"
  exit 1
fi

# ================== 并行处理阶段 ==================
echo "开始并行处理... (最大并行数: $MAX_PARALLEL_JOBS)"

# 使用 xargs 并行执行（兼容性更好）
export MAPPING_FILE="/tmp/file_mapping_$$.txt"
cat nih_image_hash.txt | xargs -I {} -P $MAX_PARALLEL_JOBS bash -c "
  $(declare -f copy_single_image)
  copy_single_image \"{}\" \"$MAPPING_FILE\"
" > /tmp/rclone_copy_status_$$.txt

# ================== 统计结果 ==================
if [ -f /tmp/rclone_copy_status_$$.txt ]; then
  success_count=$(grep "^SUCCESS:" /tmp/rclone_copy_status_$$.txt | wc -l)
  fail_count=$(grep "^FAIL:" /tmp/rclone_copy_status_$$.txt | wc -l)
  not_found_count=$(grep "^NOT_FOUND:" /tmp/rclone_copy_status_$$.txt | wc -l)
  
  echo "----------------------------------------"
  echo "📊 处理统计:"
  echo "   ✅ 成功: $success_count 个文件"
  echo "   ❌ 失败: $fail_count 个文件"
  echo "   ⚠️ 未找到: $not_found_count 个文件"
  echo "   📈 总计: $total_files 个文件"
  
  if [ $fail_count -gt 0 ]; then
    echo "   失败的文件:"
    grep "^FAIL:" /tmp/rclone_copy_status_$$.txt | cut -d: -f2
  fi
  
  if [ $not_found_count -gt 0 ]; then
    echo "   未找到的文件:"
    grep "^NOT_FOUND:" /tmp/rclone_copy_status_$$.txt | cut -d: -f2
  fi
fi

# ================== 清理临时文件 ==================
echo "清理临时文件..."
rm -f /tmp/preprocess_$$.py
rm -f /tmp/file_mapping_$$.txt
rm -f /tmp/rclone_copy_status_$$.txt

echo "✅ 图片处理完成！"