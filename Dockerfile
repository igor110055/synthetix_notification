FROM python:3.7

WORKDIR /app
COPY . .

RUN pip install -r env/requirements.txt

CMD ["python","main.py","-t","prod"]