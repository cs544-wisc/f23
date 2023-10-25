FROM p5-base
RUN apt-get update; apt-get install -y unzip
CMD python3 -m jupyterlab --no-browser --ip=0.0.0.0 --port=5000 --allow-root --NotebookApp.token=''
