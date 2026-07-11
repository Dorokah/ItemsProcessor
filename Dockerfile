FROM python:3.12-slim

ADD deps /deps

RUN pip install --upgrade pip && \
    pip install certifi charset-normalizer idna urllib3 packaging deprecated typing-extensions pylogbeat six future contextlib2 threadloop thrift && \
    pip install -r /deps/requirements.txt && \
    cd / && \
    rm -rf /deps && \
    mkdir /app

WORKDIR /app

ADD src /app/src

ENTRYPOINT ["python3", "-m", "src.main"]
