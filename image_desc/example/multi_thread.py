import base64
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# === 1. 配置客户端 ===
client = openai.Client(
    api_key="EMPTY",                       # vLLM 不鉴权
    base_url="http://10.140.37.39:8006/v1/"  # 换成你的 IP:端口
)

# === 2. 图片路径列表（替换为你自己的图片路径）===

# === 3. 推测 MIME 类型的辅助函数 ===
def get_mime_type(image_path):
    ext = os.path.splitext(image_path)[-1].lower()
    mime_map = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".bmp": "image/bmp",
        ".webp": "image/webp"
    }
    return mime_map.get(ext, "image/jpeg")

# === 4. 处理单张图片的函数 ===
def process_image(image_path):
    try:
        # 读取并编码图片
        with open(image_path, "rb") as f:
            base64_str = base64.b64encode(f.read()).decode("utf-8")
        mime_type = get_mime_type(image_path)

        # 构造消息
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url",
                     "image_url": {"url": f"data:{mime_type};base64,{base64_str}"}},
                    {"type": "text",
                     "text": "请根据这张图片描述，尽量贴合图片内容；要求正确识别地点、人物、景观、行为等关键要素，语言简洁凝练，不要自由发挥。"}
                ]
            }
        ]

        # 调用模型
        response = client.chat.completions.create(
            model="Qwen2.5-VL-7B-Instruct",
            messages=messages,
            max_tokens=512,
            temperature=0.7,
            stream=False
        )

        result = response.choices[0].message.content
        return image_path, result, None  # 返回结果和 None 表示无错误

    except Exception as e:
        return image_path, None, str(e)  # 返回错误信息

# === 5. 多线程处理所有图片 ===
def process_images_multithreaded(image_paths, max_workers=4):
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_path = {executor.submit(process_image, path): path for path in image_paths}

        for future in as_completed(future_to_path):
            image_path, content, error = future.result()
            if error:
                print(f"[错误] 处理 {image_path} 时出错: {error}")
                results[image_path] = {"error": error}
            else:
                print(f"[完成] {image_path}")
                results[image_path] = {"description": content}

    return results

# 时间格式化
def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"

# === 6. 执行并打印结果 ===
if __name__ == "__main__":
    image_paths = [
    "shanghai.jpeg",
    ] * 30
    import time 
    st = time.time()
    results = process_images_multithreaded(image_paths, max_workers=8)

    print("\n=== 所有图片处理完成 ===", format_time(time.time() - st))
    cnt = 0
    for img_path, res in results.items():
        print(f"\n {img_path}:")
        if "description" in res:
            print(res["description"])
        else:
            print(f"错误: {res['error']}")
        
