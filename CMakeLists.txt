cmake_minimum_required(VERSION 3.25.1)
project(ledis_server)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++20 -Wall -I /usr/local/include -L /usr/local/lib -lhttpserver")

add_executable(ledis_server main.cpp ldsDb.h ldsKey.h ldsVal.h
        ldsSnapshot.h
        ldsCmd.h
        httpResource.h
        dbGate.h
        logger.h)