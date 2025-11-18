FROM jupyter/base-notebook:latest

WORKDIR /home/jovyan

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY analysis.ipynb ./analysis.ipynb

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.notebook_dir=/home/jovyan"]
