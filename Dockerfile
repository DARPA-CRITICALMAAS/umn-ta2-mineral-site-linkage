# Dockerfile, Image, Container
FROM python:3.10

WORKDIR umn-ta2-mineral-site-linkage

ADD fusemine.py .
ADD process_data_to_schema.py .

COPY pyproject.toml poetry.lock ./

RUN pip install --upgrade pip
RUN pip install poetry
RUN poetry install

COPY . /umn-ta2-mineral-site-linkage

# ENTRYPOINT ["poetry", "run", "python3"]
# CMD ["fusemine.py", "--commodity", "--same_as_directory", "--same_as_filename", "process_data_to_schema.py", "--raw_data", "--attribute_map", "--schema_output_directory", "--schema_output_filename"] 
# Or enter the name of your unique directory and parameter set.f