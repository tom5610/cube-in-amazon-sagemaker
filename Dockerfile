
FROM opendatacube/geobase:wheels-3.0.4  as env_builder

<<<<<<< HEAD
RUN pip3 install --upgrade pip
RUN pip3 install matplotlib click scikit-image pep8 ruamel.yaml
RUN pip3 install git+https://github.com/sat-utils/sat-search.git@0.3.0-b2
RUN pip3 install --extra-index-url="https://packages.dea.ga.gov.au" odc-index
USER $NB_UID
=======
ARG py_env_path=/env

RUN mkdir -p /conf
COPY requirements.txt /conf/
RUN env-build-tool new /conf/requirements.txt ${py_env_path} /wheels

FROM opendatacube/geobase:runner-3.0.4
ARG py_env_path

COPY --from=env_builder $py_env_path $py_env_path
COPY --from=env_builder /bin/tini /bin/tini

RUN apt-get update -y \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends \
  # developer convenience
  postgresql-client-10 \
  less \
  vim \
  git \
  && rm -rf /var/lib/apt/lists/*


RUN export GDAL_DATA=$(gdal-config --datadir)
ENV LC_ALL=C.UTF-8 \
    PATH="/env/bin:$PATH"

RUN useradd -m -s /bin/bash -N jovyan
USER jovyan
>>>>>>> master

WORKDIR /notebooks

ENTRYPOINT ["/bin/tini", "--"]

CMD ["jupyter", "notebook", "--allow-root", "--ip='0.0.0.0'" "--NotebookApp.token='secretpassword'"]
