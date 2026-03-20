FROM python:3.12-alpine

COPY . /app

WORKDIR /app

RUN apk add --no-cache \
    libc6-compat \
    build-base \
    openssl-dev \
    curl \
    vim \
    && rm -rf /var/cache/apk/*

RUN chmod +x run.sh

RUN pip install --no-cache-dir --upgrade pip setuptools wheel Cython==3.1.1

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["sh", "run.sh"]
