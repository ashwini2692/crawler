FROM python:3.7-slim-stretch

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

ARG SSH_PRIVATE_KEY
ARG ENV_FILE

RUN which ssh-agent || ( apt-get update -y && apt-get install openssh-client -y )

RUN mkdir /root/.ssh/
RUN echo "${SSH_PRIVATE_KEY}" >> /root/.ssh/id_rsa \
  && chmod 600 /root/.ssh/id_rsa \
  && eval $(ssh-agent -s) \
  && echo "$SSH_PRIVATE_KEY" | ssh-add - > /dev/null \
  && echo "Host *\n\tStrictHostKeyChecking no\n\n" >> /root/.ssh/config \
  && touch /root/.ssh/known_hosts \
  && ssh-keyscan gitlab.com >> /root/.ssh/known_hosts


COPY .  /crawler
RUN echo "${ENV_FILE}" > "/crawler/.env"
WORKDIR /crawler

RUN pip install -r requirements.txt
