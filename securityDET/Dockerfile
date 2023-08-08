# 使用Python作为基础镜像
FROM python:3.9

# 将当前目录下的所有文件复制到镜像的/app目录内
COPY . /app

# 设置工作目录
WORKDIR /app

# 安装依赖
RUN pip install -r requirements.txt

# 暴露容器端口（如果你的Flask应用监听不同的端口，请将下面的端口号改为相应的端口）
EXPOSE 8080

# 运行Python项目
CMD ["python", "security.py"]