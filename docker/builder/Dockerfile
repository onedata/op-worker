FROM onedata/base:v1
MAINTAINER Konrad Zemek <konrad.zemek@gmail.com>

# Install Elixir
RUN mkdir /opt/elixir
RUN curl -LO https://github.com/elixir-lang/elixir/releases/download/v1.0.2/Precompiled.zip && \
    unzip Precompiled.zip -d /opt/elixir && \
    rm Precompiled.zip
ENV PATH /opt/elixir/bin:$PATH

# Install Globus
ADD http://toolkit.globus.org/ftppub/gt6/installers/repo/globus-toolkit-repo_latest_all.deb \
    globus-toolkit-repo_latest_all.deb
RUN dpkg -i globus-toolkit-repo_latest_all.deb && \
    apt-get update && \
    apt-get install -y libglobus-common-dev libglobus-gsi-callback-dev && \
    apt-get clean all

# Install other onedata build deps
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 828AB726 D73BB29D
RUN echo 'deb http://ppa.launchpad.net/george-edison55/cmake-3.x/ubuntu utopic main' \
         > /etc/apt/sources.list.d/cmake.list
RUN apt-get update && \
    apt-get install -y clang libbotan1.10-dev libboost-all-dev libtbb-dev \
                       cmake ninja-build subversion libfuse-dev pkg-config \
                       libltdl-dev rpm python-sphinx libstdc++-4.9-dev \
                       libprotobuf-dev protobuf-compiler && \
    apt-get clean all

RUN echo '    StrictHostKeyChecking no' >> /etc/ssh/ssh_config && \
    echo '    UserKnownHostsFile /dev/null' >> /etc/ssh/ssh_config

# Set up the environment
RUN ln -sf /usr/bin/python3 /usr/bin/python
ENV CC clang
ENV CXX clang++
