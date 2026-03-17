gunicorn app.main:app -k uvicorn.workers.UvicornWorker -w 2 --timeout 600 -b 0.0.0.0:8000 --keep-alive 10 --max-requests-jitter 100
