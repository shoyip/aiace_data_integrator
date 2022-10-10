FROM ubuntu:focal

WORKDIR /app

COPY . .

RUN ["chmod", "+x", "install_requirements.sh"]
RUN ["bash", "./install_requirements.sh"]
ENV PATH="${PATH}:/app/bin/"

CMD ["bash", "./data_cleaning.sh"]
