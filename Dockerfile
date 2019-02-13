FROM python:3
RUN adduser --disabled-password --gecos '' stdhops
ADD . /home/stdhops/app
WORKDIR /home/stdhops/app
EXPOSE 4000
RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools
RUN pip3 install numpy pandas scikit-learn geopandas
RUN pip3 install -r requirements.txt
ENTRYPOINT ["python", "index.py"]
