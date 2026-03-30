FROM python:3.8-slim

RUN apt-get update && apt-get install -y build-essential

COPY ./python_requirements.txt /opt/code-optimizer/python_requirements.txt
RUN pip3 install -r /opt/code-optimizer/python_requirements.txt

COPY . /opt/code-optimizer
WORKDIR /opt/code-optimizer

RUN make

EXPOSE 8080

ENTRYPOINT ["python3", "script/py/run_webService.py"]
