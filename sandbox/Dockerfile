FROM python:3.12-slim

RUN pip install --no-cache-dir numpy pandas ta

COPY sandbox/runner.py /sandbox/runner.py
COPY app/core/indicators.py /sandbox/indicators.py

ENV PYTHONPATH=/sandbox
USER nobody:nogroup
WORKDIR /tmp

ENTRYPOINT ["python", "/sandbox/runner.py"]
