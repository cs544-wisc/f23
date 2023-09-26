FROM ubuntu:23.10
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip install jupyterlab==4.0.3 pyarrow==13.0.0 --break-system-packages
CMD ["python3", "-m", "jupyterlab", "--no-browser", "--ip=0.0.0.0", "--port=5440", "--allow-root", "--NotebookApp.token=''"]
