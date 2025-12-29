import base64
import os
import sys
def add_image_as_base64(image_path, alt_text="性能测试图表"):
        """将本地图片转换为Base64并生成HTML img标签"""
        try:
            with open(image_path, 'rb') as image_file:
                image_data = image_file.read()
                base64_data = base64.b64encode(image_data).decode('utf-8')

            # 根据图片格式确定MIME类型
            if image_path.lower().endswith('.png'):
                mime_type = 'png'
            elif image_path.lower().endswith('.jpg') or image_path.lower().endswith('.jpeg'):
                mime_type = 'jpeg'
            elif image_path.lower().endswith('.gif'):
                mime_type = 'gif'
            else:
                mime_type = 'jpeg'  # 默认

            return f'<img src="data:image/{mime_type};base64,{base64_data}" alt="{alt_text}" style="width: 80%; height: auto;">'
        except Exception as e:
            print(f"图片处理错误: {e}")
            return f'<p>图片加载失败: {alt_text}</p>'

if __name__ == "__main__":
    img_path = sys.argv[1]
    mailfile = sys.argv[2]
    res = add_image_as_base64(img_path)
    f = open(mailfile,'a')
    f.write(res)
    f.write("\n")
    f.close()
