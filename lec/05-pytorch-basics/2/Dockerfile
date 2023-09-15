FROM ubuntu:23.10
RUN apt-get update && apt-get install -y unzip python3 python3-pip
RUN pip install pandas==2.1.0 jupyterlab==4.0.3 matplotlib --break-system-packages
RUN pip3 install torch==2.0.1 --index-url https://download.pytorch.org/whl/cpu --break-system-packages
RUN pip3 install tensorboard==2.14.0 --break-system-packages
CMD ["python3", "-m", "jupyterlab", "--no-browser", "--ip=0.0.0.0", "--port=5440", "--allow-root", "--NotebookApp.token=''"]
