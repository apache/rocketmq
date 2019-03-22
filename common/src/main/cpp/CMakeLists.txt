cmake_minimum_required(VERSION 2.6.4)

project(rocketmq CXX)

if (POLICY CMP0003)
    cmake_policy(SET CMP0003 NEW)
endif ()

if (POLICY CMP0042)
    cmake_policy(SET CMP0042 NEW)
endif ()

if (POLICY CMP0064)
    cmake_policy(SET CMP0064 NEW)
endif ()

find_package(JNI REQUIRED)

include_directories(SYSTEM ${JNI_INCLUDE_DIRS})

add_library(rocketmq_util_all SHARED org_apache_rocketmq_common_UtilAll.cpp)
target_link_libraries(rocketmq_util_all ${JAVA_JVM_LIBRARY})
set_target_properties(rocketmq_util_all
                      PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../../resources)