gunicorn app.main:app -k uvicorn.workers.UvicornWorker -w 1 --timeout 600 -b 0.0.0.0:8000 --keep-alive 10
