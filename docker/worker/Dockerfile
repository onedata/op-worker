FROM fedora:21
MAINTAINER Konrad Zemek <konrad.zemek@gmail.com>

# Get the image up to date
RUN yum update -y && \
    yum clean all

# Install Erlang & Elixir build dependencies
RUN yum install -y gcc gcc-c++ glibc-devel make ncurses-devel openssl-devel \
                   autoconf java-1.8.0-openjdk-devel tar unzip && \
    yum clean all

# Build and install Erlang
ADD https://raw.githubusercontent.com/spawngrid/kerl/master/kerl kerl
RUN chmod +x kerl
RUN ./kerl build 17.4 17.4 && \
    ./kerl install 17.4 /opt/erlang && \
    ./kerl cleanup all
ENV PATH /opt/erlang/bin:$PATH

# Runtime dependencies
RUN yum install -y protobuf && \
    yum clean all

# Set up the environment
ENV HOME /root
EXPOSE 53 5555 443 8443 80 8876 8877
