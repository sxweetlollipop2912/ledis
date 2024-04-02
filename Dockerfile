FROM gcc:latest

COPY . /usr/src/ledis
WORKDIR /usr/

# Install dependencies & libmicrohttpd
RUN apt-get update \
    && apt-get install -y  \
    cmake  \
    libzmq3-dev  \
    ninja-build  \
    libmicrohttpd-dev  \
    git  \
    && git clone https://github.com/etr/libhttpserver.git --branch 0.19.0 --depth 1

# Build libhttpserver
WORKDIR ./libhttpserver
RUN ./bootstrap && mkdir build && cd build && ../configure && make && make install
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/local/lib"

# Build ledis
WORKDIR /usr/src/ledis
RUN  cmake -DCMAKE_BUILD_TYPE=Release -G Ninja -S /usr/src/ledis -B /usr/src/ledis/cmake-build-release \
     && cmake --build /usr/src/ledis/cmake-build-release --target ledis_server -j 6 \

# Run ledis
EXPOSE 8080
CMD ["/usr/src/ledis/cmake-build-release/ledis_server"]