FROM python:3.13.1-bookworm
ENV FLASK_APP=ForestPrediction.py
RUN apt-get update && \
    apt-get install -y \
    ffmpeg \
    libsm6 \
    libxext6 \
    libgdal-dev \
    && apt-get clean
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN pip install GDAL==$(gdal-config --version)
RUN curl -fsSL https://tailscale.com/install.sh | sh
EXPOSE 8080
ENTRYPOINT [ "flask" ]
CMD [ "run","--port","8080","--host","0.0.0.0" ]