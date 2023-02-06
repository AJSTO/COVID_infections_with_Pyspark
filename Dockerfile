FROM jupyter/base-notebook
COPY . .

USER root
RUN sudo apt-get update
RUN sudo apt-get -y install gcc
RUN sudo apt-get install -y libgdal-dev g++ --no-install-recommends 
RUN sudo apt-get clean -y

USER jovyan


RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r ./requirements.txt
RUN pip install protobuf==3.19.0

# docker build -t covid_analyse .
# docker run --name covid_analyse -p 8888:8888 covid_analyse