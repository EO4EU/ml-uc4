FROM python:3.13.9-slim-trixie
ENV FLASK_APP=ForestPrediction.py
RUN apt-get update && apt-get install libsm6 libxext6 libexpat1 gdal-bin libgdal-dev -y
RUN pip install gdal==$(gdal-config --version)
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 8080
ENTRYPOINT [ "flask" ]
CMD [ "run","--port","8080","--host","0.0.0.0" ]