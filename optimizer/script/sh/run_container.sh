#!/bin/bash

if [[ "$1" = "-h" || "$1" = "--help" ]]; then
	echo "Required arguments: type of container; image tag"
	echo "Possible values of first parameter:"
	echo "	interactive:	interactive mode; calls and logs folders are mounted" 
	echo "			in ./build/"
	echo "	detached:	detached mode; calls and logs folders are mounted" 
	echo "			in ./build/"
	echo "	i_nomount:	interactive mode; no folders are mounted"
	echo "	d_nomount:	detached mode; no folders are mounted"
elif [[ "$1" = "interactive" ]]; then
	if [[ ! -d "./build/calls" ]]; then
		mkdir -p ./build/calls
	fi
	if [[ ! -d "./build/logs" ]]; then
		mkdir -p ./build/logs
	fi
	sudo docker run -it -p 8888:8080 --name ttx_optimizer_interactive \
		--mount type=bind,source="$(pwd)"/build/calls,target=/opt/code-optimizer/build/calls \
		--mount type=bind,source="$(pwd)"/build/logs,target=/opt/code-optimizer/build/logs ttx_optimizer:$2
elif [[ "$1" = "detached" ]]; then
	if [[ ! -d "./build/calls" ]]; then
		mkdir -p ./build/calls
	fi
	if [[ ! -d "./build/logs" ]]; then
		mkdir -p ./build/logs
	fi
	sudo docker run -d -p 8888:8080 --name ttx_optimizer_detached \
		--mount type=bind,source="$(pwd)"/build/calls,target=/opt/code-optimizer/build/calls \
		--mount type=bind,source="$(pwd)"/build/logs,target=/opt/code-optimizer/build/logs ttx_optimizer:$2
elif [[ "$1" = "i_nomount" ]]; then
	sudo docker run -it -p 8888:8080 --name ttx_optimizer_i_nomount \
		ttx_optimizer:$2
elif [[ "$1" = "d_nomount" ]]; then
	sudo docker run -d -p 8888:8080 --name ttx_optimizer_d_nomount \
		ttx_optimizer:$2
else
	echo "Please, specify how the container should be run"
	echo "Use -h (--help) option to get further information"
fi
