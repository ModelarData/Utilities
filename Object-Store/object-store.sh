#!/bin/bash
set -e


# Variables.
MINIO_FOLDER=
MINIO_PID=
AZURITE_FOLDER=
AZURITE_PID=


# Functions.
# API: http://127.0.0.1:9000
# WebUI: http://127.0.0.1:????? # Port is randomized.
start_minio() {
    MINIO_FOLDER=$(mktemp -d)
    minio server "$MINIO_FOLDER" &
    MINIO_PID=$!
    sleep 1 # Simple way to ensure azurite has time to start.
    mcli alias set local http://127.0.0.1:9000 minioadmin minioadmin
    mcli mb local/modelardb
}

stop_minio() {
    if [ ! -z "$MINIO_FOLDER" ]
    then
	rm -rf "$MINIO_FOLDER"
	kill "$MINIO_PID"
    fi
}

# Azurite Blob service is starting at http://127.0.0.1:10000
# Azurite Blob service is successfully listening at http://127.0.0.1:10000
# Azurite Queue service is starting at http://127.0.0.1:10001
# Azurite Queue service is successfully listening at http://127.0.0.1:10001
# Azurite Table service is starting at http://127.0.0.1:10002
# Azurite Table service is successfully listening at http://127.0.0.1:10002
start_azurite() {
    AZURITE_FOLDER=$(mktemp -d)
    azurite -l "$AZURITE_FOLDER" --silent --skipApiVersionCheck &
    AZURITE_PID=$!
    sleep 1 # Simple way to ensure azurite has time to start.
    az storage container create -n modelardb --connection-string 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;'
}

stop_azurite() {
    if [ ! -z "$AZURITE_FOLDER" ]
    then
	kill "$AZURITE_PID"
	rm -rf "$AZURITE_FOLDER"
    fi
}

print_error_message() {
    printf "\033[1;31mERROR: \033[39m$1\n"
}

print_arrow_message() {
  printf "\033[1;92m==> \033[39m$1 \033[m\n"
}

print_arrow_message_and_run() {
    print_arrow_message "$1"
    $1
    sleep 1
    echo
}

check_for_dependencies() {
    if [[ -n $ZSH_VERSION ]]; then
	if builtin whence -p azurite az minio mcli &> /dev/null; then
	    return # Returns as all dependencies are installed and the shell is ZSH.
	fi
    elif [[ -n $BASH_VERSION ]]; then
	if builtin type -P azurite az minio mcli &> /dev/null; then
	    return # Returns as all dependencies are installed and the shell is Bash.
	fi
    fi
    print_error_message "requires Bash or ZSH and azurite, az, minio, and mcli"
    return 1
}

# Main.
check_for_dependencies

trap stop_minio EXIT
trap stop_azurite EXIT

print_arrow_message_and_run "start_minio"
print_arrow_message_and_run "start_azurite"

print_arrow_message "waiting"
read -p "Started object stores, press enter to stop."
echo
