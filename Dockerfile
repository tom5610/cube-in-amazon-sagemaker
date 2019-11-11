FROM opendatacube/jupyter

USER root

RUN pip3 install matplotlib click scikit-image pep8 ruamel.yaml awscli

USER $NB_UID

WORKDIR /notebooks

CMD jupyter notebook
