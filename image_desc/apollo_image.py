import json
import boto3
import time
import logging
import base64
import imghdr
from io import BytesIO
from PIL import Image
import asyncio
from openai import AsyncClient
from tqdm.asyncio import tqdm_asyncio
import re
import io


# =============================
# 配置区域
# =============================
S3_CONFIG = {
    "aws_access_key_id": "C6360C346C985CF1EBE5",
    "aws_secret_access_key": "9DH4qbFGepkU8P96pP4kG7d0V14AAAGWbJhc8VJ1",
    "endpoint_url": "http://d-ceph-ssd-inside.pjlab.org.cn",
}

# BUCKET_NAME = 'heta'
# INPUT_PREFIX = 'test/element/30/ecnu/en_web_brookings/brookings_html/'
# INPUT_JSONL = INPUT_PREFIX + 'jsonl/'
# INPUT_IMAGE = INPUT_PREFIX + 'image/'
# OUTPUT_IMAGE_DESC = INPUT_PREFIX + 'image_desc/'

BUCKET_NAME = 'heta'
INPUT_PREFIX = 'element/ecnu/Apollo/Apollo_pdf/'
INPUT_JSONL = INPUT_PREFIX + 'jsonl/'
INPUT_IMAGE = INPUT_PREFIX + 'imgs/'
OUTPUT_IMAGE_DESC = INPUT_PREFIX + 'image_desc/'



# 批次大小 
BATCH_SIZE = 1

# 日志设置
logging.getLogger("httpx").setLevel(logging.WARNING)

# === 异步客户端 ===
client = AsyncClient(
    api_key="EMPTY",
    base_url="http://10.140.37.15:8007/v1/"
)

# 最大并发请求数
MAX_CONCURRENT_REQUESTS = 20
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# =============================
# 工具函数区
# =============================
def is_valid_image(image_data) -> tuple[bool, str]:
    try:
        if not image_data:
            return False, "图片数据为空"
        image = Image.open(BytesIO(image_data))
        image.verify()
        return True, ""
    except Exception as e:
        return False, str(e)

def get_image_mime(image_data):
    img_type = imghdr.what(None, image_data)
    mime_map = {
        'jpeg': 'image/jpeg',
        'png': 'image/png',
        'gif': 'image/gif',
        'bmp': 'image/bmp',
        'webp': 'image/webp'
    }
    return mime_map.get(img_type)

# === 异步图片描述生成函数 ===
async def get_image_desc_async(image_data: bytes, ref_text: str, caption: str) -> str:
    valid, error_msg = is_valid_image(image_data)
    if not valid:
        # logging.warning(f"跳过无效图片: {error_msg}")
        return ""

    base64_str = base64.b64encode(image_data).decode("utf-8")
    mime_type = get_image_mime(image_data) or 'image/jpeg'

    caption_is_available = f"这张图片的caption是{caption}" if caption else ""
    text_prompt = (
        "请给这张图片提供说明，识别图中关键标识性元素，并推测图片标题；\n"
        f"{caption_is_available}\n"
        f"可参考文字内容：{ref_text}，但要仔细甄别出与图片相关的内容\n"
        "要求语言简洁凝练，不要描述画面布局；输出格式：标题--图片说明。"
    )

    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_str}"}},
                {"type": "text", "text": text_prompt}
            ]
        }
    ]

    try:
        async with semaphore:  # 控制并发
            response = await client.chat.completions.create(
                model="Qwen2.5-VL-72B-Instruct",
                messages=messages,
                max_tokens=1024,
                temperature=0.1,
                stream=False
            )
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"调用模型失败: {e}")
        return ""

# 时间格式化
def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"

# =============================
# 批次处理函数
# =============================
async def process_batch(s3_client, batch_file_keys, output_keys_set):
    """处理一个批次的文件，返回有效图片数量"""
    print(f"开始处理批次，包含 {len(batch_file_keys)} 个文件")
    
    # 全局任务队列和结果容器
    tasks = []
    task_metadata = []
    # key: (input_file_key, line_index), value: {"meta": ..., "processed_items": [...]}
    file_line_results = {}

    batch_start_time = time.time()
    valid_image_count = 0  # 有效图片计数器

    # 遍历批次中的所有文件，收集任务
    for file_key in batch_file_keys:
        output_key = file_key.replace(INPUT_JSONL, OUTPUT_IMAGE_DESC)
        # if output_key in output_keys_set:
        #     print(f"跳过已处理文件: {file_key} -> {output_key}")
        #     continue

        print(f"读取文件: {file_key}")
        try:
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            lines = [line.strip() for line in content.split('\n') if line.strip()]
        except Exception as e:
            logging.error(f"无法读取文件 {file_key}: {e}")
            continue

        # 处理每个JSON行
        for line_index, json_line in enumerate(lines):
            if line_index % 1000 == 0:
                print(f"  已读取 {line_index} 行...")
            try:
                data = json.loads(json_line)
            except Exception as e:
                logging.error(f"无法解析文件 {file_key} 第 {line_index} 行: {e}")
                continue

            if "json_content" not in data:
                continue

            # 保存原始meta信息
            original_meta = data.get("meta", {})
            
            # 提取所有 page_x 的键，并按数字排序
            page_keys = sorted(
                (k for k in data["json_content"].keys() if k.startswith("page_")),
                key=lambda k: int(re.search(r'\d+$', k).group())
            )

            page_texts = {}
            images_in_line = []

            for page_key in page_keys:
                page_list = data["json_content"][page_key]
                if not page_list:
                    continue  # 跳过空列表
                if page_list[-1].get("type") == "merge_text":
                    page_texts[page_key] = page_list[-1]["text"]
                images_in_line.extend(item for item in page_list if item.get("type") == "image")

            # 初始化该行的结果容器
            file_line_key = (file_key, line_index)
            file_line_results[file_line_key] = {
                "meta": original_meta,
                "original_json_content": data.get("json_content", {}),  # 保存原始结构
                "processed_items": []
            }

            for image_item in images_in_line:
                cnt = int(image_item["id"].split("_")[1])
                text = " ".join([page_texts.get(f"page_{cnt + i}", "") for i in [-1, 0, 1]])
                if "meta" in data and "description" in data["meta"]:
                    text = data["meta"]["description"] + " " + text
                image_item["desc"] = text

                # 构建任务
                if "web_url" in image_item:
                    image_key = INPUT_IMAGE + image_item["web_url"]
                elif "url" in image_item:
                    image_key = INPUT_IMAGE + image_item["url"]
                try:
                    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=image_key)
                    image_content = response['Body'].read()
                except Exception as e:
                    logging.error(f"无法读取图片 {image_key}: {e}")
                    image_item["desc"] = ""
                    file_line_results[file_line_key]["processed_items"].append(image_item)
                    continue

                ref_text = image_item["desc"]
                caption = image_item.get("caption", "")

                task = get_image_desc_async(image_content, ref_text, caption)
                tasks.append(task)
                task_metadata.append({
                    "file_line_key": file_line_key,
                    "image_item": image_item
                })

    if not tasks:
        print("该批次没有需要处理的任务")
        return 0

    print(f"该批次共收集到 {len(tasks)} 个图片任务，开始并行处理...")

    # 并行执行所有任务
    results = await tqdm_asyncio.gather(*tasks, desc="处理图片", total=len(tasks))

    # 将结果回填到对应的行结果中
    for meta, result in zip(task_metadata, results):
        file_line_key = meta["file_line_key"]
        image_item = meta["image_item"]

        if isinstance(result, Exception):
            image_item["desc"] = ""
        else:
            image_item["desc"] = result

        # 如果描述不为空，计数器加一
        if image_item["desc"].strip():
            valid_image_count += 1

        file_line_results[file_line_key]["processed_items"].append(image_item)

    # 写回每个文件的结果
    print("开始写回结果...")
    
    # 按文件分组处理结果
    file_outputs = {}
    for (file_key, line_index), result_data in file_line_results.items():
        if file_key not in file_outputs:
            file_outputs[file_key] = {}
        file_outputs[file_key][line_index] = result_data

    # 为每个文件写入结果
    for file_key, line_results in file_outputs.items():
        output_key = file_key.replace(INPUT_JSONL, OUTPUT_IMAGE_DESC)
        output_stream = BytesIO()

        # 按行顺序写入结果
        for line_index in sorted(line_results.keys()):
            result_data = line_results[line_index]
            
            # 重建json_content，只包含处理过的图片项
            new_json_content = {}
            
            # 按页面分组处理过的图片项
            for image_item in result_data["processed_items"]:
                page_id = image_item["id"].split("_")[1]
                page_key = f"page_{page_id}"
                if page_key not in new_json_content:
                    new_json_content[page_key] = []
                new_json_content[page_key].append(image_item)

            # 构建输出数据，保持原有行结构
            # 只有当new_json_content不为空时才写入
            if new_json_content:
                # 构建输出数据，保持原有行结构
                output_data = {
                    "meta": result_data["meta"],
                    "json_content": new_json_content
                }

                json_line = json.dumps(output_data, ensure_ascii=False)
                output_stream.write(json_line.encode('utf-8'))
                output_stream.write(b'\n')

        # 上传结果
        if output_stream.tell() > 0:  # 只有当有内容时才上传
            output_stream.seek(0)
            s3_client.upload_fileobj(
                Key=output_key,
                Fileobj=output_stream,
                Bucket=BUCKET_NAME,
                ExtraArgs={'ContentType': 'application/json'}
            )
            print(f"结果已上传: s3://{BUCKET_NAME}/{output_key}")

    batch_time = time.time() - batch_start_time
    print(f"批次处理完成，耗时: {format_time(batch_time)}")
    print(f"本批次有效图片数量: {valid_image_count}")
    return valid_image_count  # 返回有效图片数量

# =============================
# 主程序入口
# =============================
async def main():
    s3_client = boto3.client("s3", **S3_CONFIG)

    # 列出所有输入 JSONL 文件
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=INPUT_JSONL)
    file_keys = [
        obj['Key'] for page in pages for obj in page.get('Contents', [])
        if obj['Key'].lower().endswith('.jsonl')
    ]
    print(f"找到 {len(file_keys)} 个待处理的 JSONL 文件")

    # 列出所有输出文件
    output_keys_set = set()
    output_paginator = s3_client.get_paginator('list_objects_v2')
    output_pages = output_paginator.paginate(Bucket=BUCKET_NAME, Prefix=OUTPUT_IMAGE_DESC)
    for page in output_pages:
        for obj in page.get('Contents', []):
            output_keys_set.add(obj['Key'])
    print(f"已存在 {len(output_keys_set)} 个输出文件，将跳过已处理的输入文件")

    total_start_time = time.time()
    global_valid_count = 0  # 全局有效图片计数器

    # 分批处理文件
    for i in range(0, len(file_keys), BATCH_SIZE):
        batch = file_keys[i:i + BATCH_SIZE]
        print(f"\n=== 处理第 {i//BATCH_SIZE + 1} 批次 ({len(batch)} 个文件) ===")
        batch_valid_count = await process_batch(s3_client, batch, output_keys_set)
        global_valid_count += batch_valid_count
        print(f" 全局有效图片数量: {global_valid_count}")

    total_time = time.time() - total_start_time
    print(f"\n所有批次处理完成，总耗时: {format_time(total_time)}")
    print(f"🎉 全局有效图片总数: {global_valid_count}")

if __name__ == "__main__":
    print('******* 开始图文理解处理流程 ********')
    asyncio.run(main())