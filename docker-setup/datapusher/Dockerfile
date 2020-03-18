FROM ubuntu:18.04

RUN apt-get -q -y update \
    && DEBIAN_FRONTEND=noninteractive apt-get -q -y upgrade \
    && apt-get -q -y install python-pip python-dev python-dev python-virtualenv build-essential libxslt1-dev libxml2-dev zlib1g-dev git libffi-dev

RUN git clone --branch 0.0.15 https://github.com/ckan/datapusher.git

RUN pip install -U pip && \
    pip install --upgrade setuptools && \
    pip install --upgrade werkzeug==0.16.0 && \
    pip install -r ./datapusher/requirements.txt && \
    pip install -e ./datapusher

CMD [ "python", "datapusher/datapusher/main.py", "datapusher/deployment/datapusher_settings.py"]

