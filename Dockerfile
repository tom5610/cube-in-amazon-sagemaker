FROM opendatacube/jupyter

USER root

RUN pip3 install --upgrade pip
RUN pip3 install matplotlib click scikit-image pep8 ruamel.yaml
RUN pip3 install git+https://github.com/sat-utils/sat-search.git@0.3.0-b2
RUN pip3 install --extra-index-url="https://packages.dea.ga.gov.au" odc-index
USER $NB_UID

WORKDIR /notebooks

CMD jupyter notebook
