FROM jupyter/base-notebook:latest

USER root

# Install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER $NB_UID