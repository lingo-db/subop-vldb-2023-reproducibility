FROM ubuntu:16.04
RUN apt update
RUN apt upgrade
RUN apt install -y wget build-essential software-properties-common apt-transport-https ca-certificates
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN apt-add-repository "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-6.0 main"
RUN apt-get update
RUN apt-get install -y llvm-6.0-dev clang-6.0 curl gdb libcurl4-gnutls-dev python-pip
RUN ln -s /usr/bin/llvm-config-6.0 /usr/local/bin/llvm-config
RUN pip install setuptools
RUN pip install numpy===1.16.0
RUN pip install pandas===0.24.0
RUN pip install scipy===0.16.0
RUN pip install pygrizzly
RUN pip install pyweld
RUN pip install weldnumpy
