#!/bin/bash

real_path=$(realpath $(dirname $(readlink -f $0)))
lib_dirs="$real_path/lib"

param="$@"
echo LD_LIBRARY_PATH=$lib_dirs $param
LD_LIBRARY_PATH=$lib_dirs $param

