FROM jupyter/base-notebook:latest

USER root

# Install Graphviz system package
RUN apt-get update && \
    apt-get install -y graphviz && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER $NB_UID