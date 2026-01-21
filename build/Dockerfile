FROM python:3.13-slim-trixie AS build

ENV FLASK_APP=ForestPrediction.py

RUN apt-get update && \
    apt-get install -y \
        libsm6 \
        libxext6 \
        libexpat1 \
        gdal-bin libgdal-dev \
        libblas3 liblapack3 \
        build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN pip install gdal==$(gdal-config --version) && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /usr/local/lib/python3.13/site-packages/setuptools*

RUN groupadd -g 1000 eo4eu && \
    useradd -u 1000 -g 1000 -m eo4eu && \
    chown -R eo4eu:eo4eu /app


FROM registry.apps.eo4eu.eu/eo4eu/eo4eu-inference-server/uc4/python:3.13

ENV FLASK_APP=ForestPrediction.py
ENV LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu/blas:\
/usr/lib/x86_64-linux-gnu/lapack:/usr/lib:\
/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu"

WORKDIR /app

COPY --from=build /app /app
COPY --from=build /usr/local /usr/local
COPY --from=build /usr/lib /usr/lib/
COPY --from=build \
  /usr/local/lib/python3.13/site-packages \
  /opt/python/lib/python3.13/site-packages

EXPOSE 8080
USER 1000
ENTRYPOINT ["flask"]
CMD ["run", "--port", "8080", "--host", "0.0.0.0"]