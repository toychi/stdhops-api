FROM python:3.6-alpine
RUN adduser -D stdhops
ADD . /home/stdhops
WORKDIR /home/stdhops
EXPOSE 4000
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "index.py"]
