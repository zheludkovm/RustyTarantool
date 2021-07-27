#!/usr/bin/env bash
set -e

dir=$(dirname "$0")
cd $dir

container=rusty_tarantool_test
port=3301
user=user
pass=pass
init_script=init-tarantool-docker.lua
data_file=countries.json
# src_dir=$(realpath -s "$dir")/test-tarantool
src_dir=$(pwd)/test-tarantool
dst_dir=/opt/tarantool

_ansiReset=$'\e[0m' # https://superuser.com/questions/33914/why-doesnt-echo-support-e-escape-when-using-the-e-argument-in-macosx/33950#33950
_ansiBold=$'\e[1m'
_ansiRed=$'\e[31m'
_ansiLightGray=$'\e[37m'
_ansiDarkGray=$'\e[90m'
_ansiDim=$'\e[2m'
_ansiGreen=$'\e[32m'

err() {
	echo ${_ansiRed}${_ansiBold}ERR${_ansiReset}: "$@" >&2
	exit 1
}

x() {
	if [[ $dry_run ]]; then
		echo "${_ansiDarkGray}${_ansiBold}DRY RUN: ${_ansiLightGray}$@${_ansiReset}"
	else
		will "$@"
		"$@"
		local exitcode=$?
		if [[ exitcode -eq 0 ]]; then
			did "$@"
		else
			err
		fi
	fi
}


will() {
	echo "${_ansiDarkGray}${_ansiBold}WILL ${_ansiLightGray}$@${_ansiDarkGray} . . .${_ansiReset}"
}

did() {
	echo "${_ansiGreen}${_ansiBold}OK: ${_ansiLightGray}$@${_ansiReset}"
}

info() {
	if [[ $verbose ]]; then
		echo ${_ansiDarkGray}${_ansiBold}INFO:${_ansiReset} "$@"
	fi
}

dry_run=
verbose=
while [[ $# -gt 0 ]]; do
	case $1 in 
		--dry-run )
			dry_run=$1
		;;
		--verbose )
			verbose=$1
		;;
		-* )
			err Unexpected opt $1
		;;
		* )
			err Unexpected arg $1
		;;
	esac
	shift
done

set +e
x sudo docker container stop $container
x sudo docker container rm -f $container
set -e
x sudo docker run \
    --name $container \
    -p$port:3301 \
    -e TARANTOOL_USER_NAME=$user \
    -e TARANTOOL_USER_PASSWORD=$pass \
    -v $src_dir/$init_script:$dst_dir/$init_script \
    -v $src_dir/$data_file:$dst_dir/$data_file \
    -v $src_dir/db:$dst_dir/db \
	-d \
    tarantool/tarantool tarantool $dst_dir/$init_script
x cargo test
