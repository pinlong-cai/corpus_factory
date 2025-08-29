import asyncio
import base64
from openai import AsyncClient


CNT = 0
CNT_LOCK = asyncio.Lock()  # 异步锁，防止竞态
# 创建异步客户端
client = AsyncClient(
    api_key="EMPTY",
    base_url="http://10.140.37.39:8006/v1/"
)

def encode_image(image_path):
    with open(image_path, "rb") as f:
        base64_str = base64.b64encode(f.read()).decode("utf-8")
    ext = image_path.split(".")[-1]
    mime = f"image/{ext.lower()}"
    return base64_str, mime

async def ask_model(image_path, cnt):
    base64_str, mime_type = encode_image(image_path)
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_str}"}},
                {"type": "text", "text": "请一段话描述这张图片，如果含有地点、人物、行为等要素，需要准确识别； 要求语言简洁凝练。"}
            ]
        }
    ]
    response = await client.chat.completions.create(
        model="Qwen2.5-VL-7B-Instruct",
        messages=messages,
        max_tokens=512,
        temperature=0.7
    )
    async with CNT_LOCK:
        global CNT
        current_cnt = CNT
        print(f"Result {current_cnt + 1}: {response.choices[0].message.content}")
        CNT += 1
    return response.choices[0].message.content

# 时间格式化
def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"
# 并行发送多个请求
async def main():
    tasks = [
        ask_model("shanghai.jpeg", 1),
        ask_model("shanghai.jpeg", 2),
        ask_model("shanghai.jpeg", 3),
        ask_model("shanghai.jpeg", 4),
        ask_model("shanghai.jpeg", 5),
        ask_model("shanghai.jpeg", 6),
        ask_model("shanghai.jpeg", 7),
        ask_model("shanghai.jpeg", 8),
        ask_model("shanghai.jpeg", 9),
        ask_model("shanghai.jpeg", 10),
    ] 
    results = await asyncio.gather(*tasks)
    # for i, r in enumerate(results):
    #     print(f"Result {i+1}:\n{r}\n---")

import time 
st = time.time()
for i in range(3):
    asyncio.run(main())
print("\n=== 所有图片处理完成 ===", format_time(time.time() - st))