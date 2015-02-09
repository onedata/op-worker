FROM ubuntu:14.10
MAINTAINER Konrad Zemek <konrad.zemek@gmail.com>

# Add custom package sources
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys D73BB29D
RUN echo 'deb http://ppa.launchpad.net/lengau/protobuf/ubuntu utopic main' \
         > /etc/apt/sources.list.d/protobuf.list

# Get the image up to date
RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get clean all

# Install basic packages
RUN apt-get install -y curl git openssl rsync libprotobuf9 unzip

# Add an Erlang-building script
ADD https://raw.githubusercontent.com/spawngrid/kerl/master/kerl kerl
RUN chmod +x kerl

# Install & build Erlang
RUN apt-get install -y build-essential libncurses5-dev libssl-dev && \
    ./kerl build 17.4 17.4 && \
    ./kerl install 17.4 /opt/erlang && \
    ./kerl cleanup all && \
    apt-get purge -y build-essential libncurses5-dev libssl-dev && \
    apt-get autoremove -y && \
    apt-get clean all

# Set up the environment
RUN ln -s /usr/bin/python3 /usr/bin/python
ENV PATH /opt/erlang/bin:$PATH
ENV HOME /root
