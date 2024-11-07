# Dockerfile, Image, Container
FROM python:3.10

WORKDIR /umn-ta2-mineral-site-linkage

ADD fusemine.py .
ADD process_data_to_schema.py .

COPY pyproject.toml poetry.lock ./

RUN pip install poetry
RUN poetry install

ENTRYPOINT ["poetry", "run", "python", "./fusemine.py"]
CMD ["--commodity"] 
# Or enter the name of your unique directory and parameter set.f