FROM ubuntu:bionic

RUN apt-get -q -y update \
    && DEBIAN_FRONTEND=noninteractive apt-get -q -y upgrade \
    && apt-get -q -y install \
        python-dev \
        python-pip \
        python-virtualenv \
        python-wheel \
        libpq-dev \
        git \
    && apt-get -q clean

COPY . /home/

RUN pip install setuptools==36.1
RUN pip install -r /home/evaluation/requirements.txt
RUN cd /home && \
    python setup-evaluation.py build && \
    python setup-evaluation.py install
