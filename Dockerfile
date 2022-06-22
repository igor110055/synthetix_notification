FROM python:3.7

WORKDIR /app
COPY . .

RUN pip3 install --upgrade discord.py --no-dependencie
RUN pip install -r env/requirements.txt

CMD ["python3","main.py","-t","prod"]
