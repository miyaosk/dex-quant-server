FROM python:3.12-alpine

RUN adduser -D -u 65534 sandbox && \
    pip install --no-cache-dir numpy pandas ta

COPY runner.py /runner.py

USER sandbox
WORKDIR /tmp

ENTRYPOINT ["python", "/runner.py"]
