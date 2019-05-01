FROM python:3
WORKDIR /app
COPY . /app/
RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools
RUN pip3 install -r requirements.txt
EXPOSE 4000
ENTRYPOINT ["python", "index.py"]
