#!/bin/bash

# =============== 配置区域 ===============
DRY_RUN=false

# rclone remote 名称（需提前通过 rclone config 配置好）
REMOTE="s3"  # 对应 rclone config 中的名字

BUCKET="heta"
BASE_PREFIX="raw/ccid/SEMI_TW"               # 源路径前缀
TAR_PREFIX="element/ccid/semi_tw"            # 目标路径前缀

# 构建路径（rclone 格式：remote:/bucket/path）
PDF_SRC_BASE="${REMOTE}://${BUCKET}/${BASE_PREFIX}"
IMAGE_DEST="${REMOTE}://${BUCKET}/${TAR_PREFIX}/semi_tw_html/image"
PDF_DEST="${REMOTE}://${BUCKET}/${TAR_PREFIX}/semi_tw_pdf"

# # 其他 rclone 参数（如自定义 endpoint）
# RCLONE_EXTRA_ARGS=(
#   "--s3-endpoint" "http://d-ceph-ssd-inside.pjlab.org.cn"
#   "--no-check-certificate"  # 如果 endpoint 是 HTTP 或自签名证书
# )

# ================== 辅助函数 ==================
run_cmd() {
  echo "[CMD] $*"
  if ! $DRY_RUN; then
    "$@"
  fi
}

# ================== 获取单位目录列表 ==================
echo "开始获取单位目录列表..."
echo "源路径: ${PDF_SRC_BASE}"
echo "图片目标: $IMAGE_DEST"
echo "PDF 目标: $PDF_DEST"
echo "预览模式: $DRY_RUN"
echo "----------------------------------------"

# 使用 rclone 列出 BASE_PREFIX 下的一级子目录（即单位名）
readarray -t unit_paths < <(
  ../../rclone-v1.68.1-linux-amd64/rclone lsf "$PDF_SRC_BASE/" \
    --dirs-only \
    --max-depth 1
)

echo "unit_paths 内容："
printf '%s\n' "${unit_paths[@]}"

# 提取单位名（去掉尾部斜杠）
units=()
for dir_name in "${unit_paths[@]}"; do
  dir_name="${dir_name%/}"
  if [[ -n "$dir_name" ]]; then
    units+=("$dir_name")
    echo "发现单位: $dir_name"
  fi
done

# 去重
readarray -t units < <(printf '%s\n' "${units[@]}" | sort -u)

if [ ${#units[@]} -eq 0 ]; then
  echo "⚠️ 未找到任何单位目录，请检查 BASE_PREFIX 是否正确。"
  exit 1
fi

echo "📌 最终单位列表: ${units[*]}"
echo "----------------------------------------"

# ================== 1. 同步 PDF 文件 ==================
# echo "📌 正在处理 PDF 文件..."
# for unit in "${units[@]}"; do
#   src_path="${PDF_SRC_BASE}/${unit}/media/pdf/"
#   dest_path="${PDF_DEST}/"
#   echo "同步 PDF: $src_path -> $dest_path"
#   
#   # 检查源目录是否存在文件
#   if ../../rclone-v1.68.1-linux-amd64/rclone lsf "$src_path" --max-depth 1 --files-only >/dev/null 2>&1; then
#     run_cmd timeout 300 ../../rclone-v1.68.1-linux-amd64/rclone copy "$src_path" "$dest_path" \
#       --include "*.pdf" \
#       --create-empty-src-dirs=false \
#       --progress
#   else
#     echo "⚠️ PDF源目录不存在或为空，跳过: $src_path"
#   fi
# done

# ================== 2. 同步 图片 文件 ==================
echo "📌 正在处理图片文件..."

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


for unit in "${units[@]}"; do
  for dir_name in "${!image_dirs[@]}"; do
    src_path="${PDF_SRC_BASE}/${unit}/media/${dir_name}/"
    echo "检查图片路径: $src_path"

    # 检查源目录是否存在文件
    if ../../rclone-v1.68.1-linux-amd64/rclone lsf "$src_path" --max-depth 1 --files-only >/dev/null 2>&1; then
      echo "同步图片: $src_path -> $IMAGE_DEST/"
      run_cmd timeout 300 ../../rclone-v1.68.1-linux-amd64/rclone copy "$src_path" "$IMAGE_DEST/" \
        --progress --transfers 200 --checkers 200 --create-empty-src-dirs=false --ignore-errors
    else
      echo "⚠️ 源目录不存在或为空，跳过: $src_path"
    fi
  done
done

echo "✅ 所有符合条件的文件已处理完成！"