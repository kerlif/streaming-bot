FROM python:3.11-slim

WORKDIR /app

# ffmpeg + procps (pgrep للـ healthcheck). لا نثبّت yt-dlp من apt (نسخته قديمة).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# --upgrade: دائماً أحدث yt-dlp عند كل build (استخدم --no-cache عند إعادة البناء)
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY streaming_bot.py healthcheck.py ./

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATABASE_PATH=/app/data/stream_manager.db

# مجلد البيانات يُنشأ بصلاحيات botuser، فالـ named volume في compose يرث الملكية
RUN useradd -r -u 1000 -d /app botuser \
    && mkdir -p /app/data \
    && chown -R botuser:botuser /app

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python /app/healthcheck.py || exit 1

USER botuser
CMD ["python", "streaming_bot.py"]
