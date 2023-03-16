FROM ubuntu:16.04
RUN apt update
RUN apt upgrade
RUN apt install -y wget build-essential software-properties-common apt-transport-https ca-certificates
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN apt-add-repository "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-6.0 main"
RUN apt-get update
RUN apt-get install -y llvm-6.0-dev clang-6.0 curl git zlib1g-dev gdb libcurl4-gnutls-dev
RUN ln -s /usr/bin/llvm-config-6.0 /usr/local/bin/llvm-config
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup update stable
RUN git clone https://gitlab.db.in.tum.de/jungmair/weld-fork.git /weld
WORKDIR /weld
RUN git checkout v0.2.0
ENV WELD_HOME=/weld
RUN cargo build --release
RUN wget -qO- "https://cmake.org/files/v3.25/cmake-3.25.0-linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local
RUN mkdir /experiments
WORKDIR /experiments
COPY CMakeLists.txt .
COPY main.cpp .
COPY experiments ./experiments
RUN mkdir build
RUN cmake . -B build -DCMAKE_BUILD_TYPE=Release
RUN cmake --build build