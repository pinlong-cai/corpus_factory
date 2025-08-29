import base64
import openai

# === 1. 配置客户端 ===
client = openai.Client(
    api_key="EMPTY",                       # vLLM 不鉴权
    base_url="http://10.140.37.15:8007/v1/"  # 换成你的 IP:端口
    # base_url="http://10.140.37.39:8006/v1/"
)

# === 2. 把本地图片编码为 base64 ===
image_path = "shanghai.jpeg"               # 换成你自己的图片
with open(image_path, "rb") as f:
    base64_str = base64.b64encode(f.read()).decode("utf-8")
mime_type = "image/jpeg"                  # 根据实际后缀改：png -> image/png

# === 3. 构造多模态 messages ===
messages = [
    {
        "role": "user",
        "content": [
            {"type": "image_url",
             "image_url": {"url": f"data:{mime_type};base64,{base64_str}"}},
            {"type": "text",
             "text": "请给这张图片提供说明，识别图中关键标识性元素，并推测图片标题；要求语言简洁凝练，不要描述画面布局；输出格式：标题--图片说明。"}
        ]
    }
]

# === 4. 请求模型 ===
response = client.chat.completions.create(
    model="Qwen2.5-VL-72B-Instruct",
    # model="Qwen2.5-VL-7B-Instruct",
    messages=messages,
    max_tokens=1024,
    temperature=0.1,
    stream=False
)

# === 5. 输出 ===
print(response.choices[0].message.content)