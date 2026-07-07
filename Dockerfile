FROM python:3.12-slim

ADD deps /deps

RUN pip install --upgrade pip && \
    pip install tornado==6.5.7 jaeger-client==4.8.0 && \
    pip install certifi charset-normalizer idna urllib3 packaging deprecated typing-extensions pylogbeat six future contextlib2 threadloop thrift && \
    pip install --no-deps opentracing-instrumentation==3.3.1 && \
    pip install --no-deps -r /deps/requirements.txt && \
    cd / && \
    rm -rf /deps && \
    mkdir /app

WORKDIR /app

ADD src /app/src

ENTRYPOINT ["python3", "-m", "src.main"]