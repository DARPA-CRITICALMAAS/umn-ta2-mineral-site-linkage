# Dockerfile, Image, Container
FROM python:3.11

WORKDIR umn-ta2-mineral-site-linkage

ADD fusemine.py .

COPY pyproject.toml poetry.lock ./

RUN pip install --upgrade pip
RUN pip install poetry
RUN poetry install

COPY . /umn-ta2-mineral-site-linkage