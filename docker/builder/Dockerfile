FROM fedora:21
MAINTAINER Konrad Zemek <konrad.zemek@gmail.com>

# Get the image up to date
RUN yum update -y && \
    yum clean all

# Install Erlang & Elixir build dependencies
RUN yum install -y gcc gcc-c++ glibc-devel make ncurses-devel openssl-devel \
                   autoconf java-1.8.0-openjdk-devel tar unzip && \
    yum clean all

# Install Globus
ADD http://toolkit.globus.org/ftppub/gt6/installers/repo/globus-toolkit-repo-latest.noarch.rpm globus-toolkit-repo-latest.noarch.rpm
RUN yum install -y globus-toolkit-repo-latest.noarch.rpm && \
    yum install -y globus-common-devel globus-gsi-callback-devel && \
    yum clean all

# Install other onedata build deps
RUN yum install -y git clang-analyzer botan-devel boost-devel tbb-devel \
                   cmake ninja-build subversion fuse-devel protobuf-devel \
                   libtool-ltdl-devel clang boost-static rpm-build \
                   python-sphinx && \
    ln -s /usr/bin/ninja{-build,} && \
    yum clean all

# Build and install Erlang
ADD https://raw.githubusercontent.com/spawngrid/kerl/master/kerl kerl
RUN chmod +x kerl
RUN ./kerl build 17.4 17.4 && \
    ./kerl install 17.4 /opt/erlang && \
    ./kerl cleanup all
ENV PATH /opt/erlang/bin:$PATH

# Install Elixir
RUN mkdir /opt/elixir
RUN curl -LO https://github.com/elixir-lang/elixir/releases/download/v1.0.2/Precompiled.zip && \
    unzip Precompiled.zip -d /opt/elixir && \
    rm Precompiled.zip
ENV PATH /opt/elixir/bin:$PATH

# Set up the environment
ENV CC clang
ENV CXX clang++
