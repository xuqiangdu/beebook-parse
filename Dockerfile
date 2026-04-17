FROM python:3.11-slim

WORKDIR /app

# 系统依赖：
#   poppler-utils —— pdftotext 二进制（PDF 备用解析引擎）
#   libmagic1     —— python-magic 的依赖（/api/parse 做 MIME 检测时用）
RUN apt-get update && apt-get install -y --no-install-recommends \
        poppler-utils libmagic1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY . .

RUN mkdir -p /app/uploads /app/books

EXPOSE 5555

# v2: 使用 gunicorn 生产级 WSGI server
#   -w 1            单 worker 进程(状态/计数器进程内统一)
#   -k gthread      线程模型(配合内部 ThreadPool)
#   --threads 8     HTTP 处理线程数(只接请求,不做解析)
#   --timeout 0     关掉 gunicorn 自身超时,交给 watchdog 管
#   --access-logfile -    访问日志输出到 stdout(docker logs 可见)
CMD ["gunicorn", "-w", "1", "-k", "gthread", "--threads", "8", \
     "--timeout", "0", "--access-logfile", "-", \
     "-b", "0.0.0.0:5555", "app:app"]
