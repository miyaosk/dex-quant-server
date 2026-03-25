FROM python:3.12-slim

RUN useradd -r -u 65534 -s /usr/sbin/nologin sandbox && \
    pip install --no-cache-dir numpy pandas ta

COPY sandbox/runner.py /sandbox/runner.py
COPY app/core/indicators.py /sandbox/indicators.py

ENV PYTHONPATH=/sandbox
USER sandbox
WORKDIR /tmp

ENTRYPOINT ["python", "/sandbox/runner.py"]
