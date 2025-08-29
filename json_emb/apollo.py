import json
import numpy as np
from typing import List, Dict, Any, Tuple
from sentence_transformers import SentenceTransformer
import torch
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError
import time
import logging
import hashlib
from datetime import datetime
from io import BytesIO


# 或者直接禁用所有日志
logging.getLogger('sentence_transformers').disabled = True
logging.getLogger("sentence_transformers").setLevel(logging.WARNING)

# =============================
# 配置区域
# =============================
# S3 配置
S3_CONFIG = {
    "aws_access_key_id": "",  # 补充id
    "aws_secret_access_key": "", # 补充key
    "endpoint_url": "", # 补充end_point
}

# --- 配置部分 ---
BUCKET_NAME = 'heta'
INPUT_PREFIX = 'element/ecnu/Apollo/Apollo_pdf/jsonl/'
OUTPUT_PREFIX = 'element/ecnu/Apollo/text_embedding/'
MODEL_NAME = "../models/bge-m3" 

# 并行配置
NUM_GPU_DEVICES = 8
MAX_WORKERS = NUM_GPU_DEVICES
EMBEDDING_BATCH_SIZE = 512

# 日志设置
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# --- 配置结束 ---


# =============================
# 全局模型缓存（每个进程独立）
# =============================
_st_model = None
_worker_gpu_id = "unknown"  # 记录当前 worker 的 GPU ID
all_emb_cnt = 0


def init_worker():
    """每个进程加载一次 SentenceTransformer 模型，并自动分配 GPU ID"""
    global _st_model, _worker_gpu_id
    try:
        pid = os.getpid()

        # === 分配 GPU ID ===
        gpu_id = pid % NUM_GPU_DEVICES
        # gpu_id = pid
        _worker_gpu_id = str(gpu_id)
        # 根据分配的 GPU ID 设置 PyTorch 的 CUDA 设备
        device = torch.device(f'cuda:{gpu_id}' if torch.cuda.is_available() else 'cpu')
        # logger.info(f"Worker (PID={pid}) 开始初始化，分配 GPU: {gpu_id}, 使用设备: {device}")
        
        _st_model = SentenceTransformer(MODEL_NAME, device=device, trust_remote_code=True)        
        _st_model.half() # 启用 FP16 (如果模型和 GPU 支持)        
        # _st_model = torch.compile(_st_model) # 或者使用 torch.compile (PyTorch 2.0+)        
        
        logger.info(f"Worker (PID={pid}) 初始化完成，SentenceTransformer 模型已加载到 GPU: {gpu_id} (设备: {device})")
    except Exception as e:
        logger.error(f"Worker (PID={pid}) 初始化失败: {e}")
        raise


# =============================
# 文本分块函数（已修正 meta_info 逻辑）
# =============================
def split_text_with_overlap(text: str, chunk_size: int = 1024, overlap: int = 50) -> List[str]:
    # meta_info = ""  # 不加入meta_info # <-- 移除或注释掉
    if len(text) <= chunk_size:
        return [text] # <-- 直接返回 text
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end] # <-- 移除 meta_info 拼接
        chunks.append(chunk)
        if end >= len(text):
            break
        start = end - overlap
    return chunks


# =============================
# 嵌入函数 (使用 SentenceTransformer) (已修正 batch_size 使用)
# =============================
def embedding(texts: List[str], model=None, batch_size: int = 4098) -> List[List[float]]: 
    if model is None:
        model = _st_model
    if model is None:
        raise RuntimeError("SentenceTransformer 模型未初始化！")
    # 使用模型的 encode 方法
    embeddings = model.encode(
        texts,
        batch_size=batch_size,  # <-- 使用传入的 batch_size
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False
    )
    return embeddings.astype(float).tolist()


# =============================
# 处理单个 JSON 对象 (已修正函数调用)
# =============================
def process_json_data_to_texts(data: Dict[str, Any]) -> Tuple[List[str], List[int], dict[str, Any]]:
    """
    仅解析 JSON 并分块，返回 meta 和 texts 列表，不生成 embedding
    """
    p_cnt = 0
    multipage_texts = []
    text_nums_per_page_list = []
    while("json_content" in data and f"page_{p_cnt}" in data["json_content"]):        
        try:
            merge_text_dic = data["json_content"][f"page_{p_cnt}"][-1]
            merge_text = merge_text_dic["text"].replace("\n", ",")
        except:
            merge_text = ""
        # meta_info = data["meta"]["description"][:100].replace("\n", ",") # 未使用
        # texts = split_text_with_overlap(merge_text, meta_info) # <-- 修改调用
        texts = split_text_with_overlap(merge_text) # <-- 修改调用
        multipage_texts.extend(texts)
        text_nums_per_page_list.append(len(texts))
        p_cnt += 1
    return multipage_texts, text_nums_per_page_list, data.get("meta", {})


# =============================
# 新增：批量处理函数（供进程池调用）—— 核心优化 (已修正 meta 顺序)
# =============================
def process_batch_s3(json_lines_batch: List[str]) -> Tuple[List[str], str]:
    global _st_model, _worker_gpu_id
    all_emb_cnt = 0
    if _st_model is None:
        init_worker()  # 确保模型已加载
    gpu_id = _worker_gpu_id
    results = []
    # === Step 1: 解析所有行，提取多页的合并文本片段 ===
    all_texts = []
    batch_text_nums = []
    batch_metas = []
    for json_line in json_lines_batch:
        try:
            data = json.loads(json_line)
        except:
            data = {}
        multipage_texts, text_nums_per_page_list, meta = process_json_data_to_texts(data)
        all_texts.extend(multipage_texts)
        batch_text_nums.append(text_nums_per_page_list)
        batch_metas.append(meta)
    # === Step 2: 批量生成 embeddings ===
    if all_texts:
        try:
            bge_m3_embeddings = embedding(all_texts, model=_st_model, batch_size=EMBEDDING_BATCH_SIZE)
        except Exception as e:
            logger.warning(f"批量 embedding 失败，降级为逐行处理: {e}")
            bge_m3_embeddings = []
            for text in all_texts:
                try:
                    emb = embedding([text], model=_st_model, batch_size=EMBEDDING_BATCH_SIZE)[0]
                    bge_m3_embeddings.append(emb)
                except:
                    emb = [0.0] * 1024
                    bge_m3_embeddings.append(emb)
    else:
        bge_m3_embeddings = []
    all_emb_cnt += len(bge_m3_embeddings)
    # === Step 3: 重组结果 ===
    emb_idx = 0
    for idx, text_nums_per_page_list in enumerate(batch_text_nums): # <-- 使用 enumerate 获取索引
        meta = batch_metas[idx]
        embedding_list = []
        for i in range(len(text_nums_per_page_list)):
            texts = all_texts[emb_idx:emb_idx + text_nums_per_page_list[i]]
            chunk_embeddings = bge_m3_embeddings[emb_idx:emb_idx + text_nums_per_page_list[i]]
            embedding_list.extend([
                {
                    "type": "text",
                    "page": i,
                    "text": text,
                    "bge_m3_embedding": emb
                }
                for text, emb in zip(texts, chunk_embeddings)
                if len(text) > 0 # <-- 添加过滤条件
            ])
            emb_idx = emb_idx + text_nums_per_page_list[i]
        
        result = {
            "original_file": meta.get("original_file", ''),
            "generated_file": meta.get("generated_file", ''),
            "timestamp": datetime.now().isoformat(),
            "total_pages": meta.get("total_pages", ''),
            "file_type": meta.get("file_type", ''),
            "url": meta.get("url", ''),
            "description": meta.get("description", ''),
            "embedding_list": embedding_list
        }
        results.append(json.dumps(result, ensure_ascii=False))
    return results, gpu_id, all_emb_cnt


def format_time(seconds):
    """将秒数转换为 h min s 格式"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"


# =============================
# 主流程：S3 流式处理 (已修正进度计数)
# =============================

def batched(iterable, batch_size):
    """将可迭代对象按 batch_size 分批，返回生成器"""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, batch_size))
        if not chunk:
            break
        yield chunk


def read_lines(body_iter):
    """流式读取 S3 响应的每一行，返回解码后的非空行"""
    for line in body_iter:
        decoded = line.decode('utf-8').strip()
        if decoded:
            yield decoded


def create_batches_by_bytes(lines, max_batch_bytes=10 * 1024 * 1024):
    """
    根据每行的 UTF-8 字节数动态分 batch
    """
    batches = []
    current_batch = []
    current_size = 0
    for line in lines:
        line_bytes = len(line.encode('utf-8'))
        # 如果当前 batch 不为空，且加入当前行会超限，则先保存当前 batch
        if current_batch and current_size + line_bytes > max_batch_bytes:
            batches.append(current_batch)
            current_batch = []
            current_size = 0
        current_batch.append(line)
        current_size += line_bytes
    # 添加最后一个 batch
    if current_batch:
        batches.append(current_batch)
    return batches


def main():
    total_emb_count = 0
    BATCH_SIZE_GPU = 1024
    s3_client = boto3.client("s3", **S3_CONFIG) # 假设 S3_CONFIG 已定义

    # === 1. 列出所有输入 JSONL 文件 ===
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=INPUT_PREFIX)
    file_keys = [
        obj['Key'] for page in pages for obj in page.get('Contents', [])
        if obj['Key'].lower().endswith('.jsonl')
    ]
    logger.info(f"找到 {len(file_keys)} 个待处理的 JSONL 文件")

    # === 2. 获取已存在的输出文件（跳过已处理）===
    output_keys_set = set()
    output_paginator = s3_client.get_paginator('list_objects_v2')
    output_pages = output_paginator.paginate(Bucket=BUCKET_NAME, Prefix=OUTPUT_PREFIX)
    for page in output_pages:
        for obj in page.get('Contents', []):
            output_keys_set.add(obj['Key'])
    logger.info(f"已存在 {len(output_keys_set)} 个输出文件，将跳过已处理的输入文件")

    file_cnt = 0
    start_time = time.time()
    with ProcessPoolExecutor(max_workers=MAX_WORKERS, initializer=init_worker) as executor:
        for key in file_keys:
            file_cnt += 1
            output_key = key.replace(INPUT_PREFIX, OUTPUT_PREFIX)
            if output_key in output_keys_set:
                logger.info(f"跳过已处理文件: {key} -> {output_key}")
                continue
            sub_start_time = time.time()
            logger.info(f"正在处理 S3 文件: {key}")
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            
            # === 流式读取 S3 文件 === 效率太低
            # lines_iter = read_lines(response['Body'].iter_lines())
            # line_batches = batched(lines_iter, BATCH_SIZE_GPU)

            # 读取整个对象内容为字节
            content = response['Body'].read().decode('utf-8')
            lines = [line.strip() for line in content.splitlines() if line.strip()]            
            logger.info(f"读取 S3 文件: {key} 完毕，耗时：{format_time(time.time() - sub_start_time)}")
            # --- 根据固定的BATCH_SIZE_GPU划分，有可能遇到太长的行oom，如果太短的行则性能不高
            # line_batches = [lines[i:i + BATCH_SIZE_GPU] for i in range(0, len(lines), BATCH_SIZE_GPU)]
            # logger.info(f"分隔 S3 文件: {key} 完毕，耗时：{format_time(time.time() - sub_start_time)}")
            
            # 使用字节数控制 batch 大小（例如 10MB）
            MAX_BATCH_BYTES = 10 * 1024 * 1024  # 10MB per batch
            # line_batches = create_batches_by_bytes(lines, max_batch_bytes=MAX_BATCH_BYTES)
            batch_info_list = [(batch, len(batch)) for batch in create_batches_by_bytes(lines, max_batch_bytes=MAX_BATCH_BYTES)] # 记录每个 batch 及其大小
            logger.info(f"分割 S3 文件: {key} 完毕，共 {len(batch_info_list)} 个 batch，耗时：{format_time(time.time() - sub_start_time)}")

            # === 准备输出流（边处理边写入）===
            output_stream = BytesIO()
            processed_line_count = 0 # 累计处理的行数
            # 提交所有 batch
            futures = {executor.submit(process_batch_s3, batch): batch_size for batch, batch_size in batch_info_list} 
            for future in as_completed(futures):
                batch_results, gpu_id, all_emb_cnt = future.result()
                batch_size = futures[future] # 获取该 batch 的大小
                # 边处理边写入 BytesIO
                for result in batch_results:
                    output_stream.write(result.encode('utf-8'))
                    output_stream.write(b'\n')
                processed_line_count += batch_size # <-- 累加处理的行数
                # 每完成一个 batch 就更新日志
                elapsed = time.time() - start_time
                elapsed_sub = time.time() - sub_start_time
                total_emb_count += all_emb_cnt
                logger.info(
                    f"正在处理：{file_cnt}/{len(file_keys)}, "
                    f"当前进度: {processed_line_count}/{len(lines)} 行, " # <-- 新变量
                    f"emb数：{total_emb_count}, "
                    f"总时间： {format_time(elapsed)}, "
                    f"当前文件时间： {format_time(elapsed_sub)}, "
                    f"GPU: {gpu_id}"
                )

            # === 上传结果（自动分段上传）===
            output_stream.seek(0)
            s3_client.upload_fileobj(
                Key=output_key,
                Fileobj=output_stream,
                Bucket=BUCKET_NAME,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'ChecksumAlgorithm': 'SHA256'  
                }
            )
            logger.info(f"文件 {key} 处理完成，目前已生成 {total_emb_count} 个 embedding。")
            logger.info(f"结果已上传: s3://{BUCKET_NAME}/{output_key}")

            # === 记录已处理 ===
            output_keys_set.add(output_key)

    logger.info("所有文件处理完成。")


if __name__ == "__main__":
    print('******* 开始文本embedding处理流程 ********')
    main()
