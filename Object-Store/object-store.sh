# This Bash/ZSH script simplify running tests that use Azurite and/or MinIO.
# It purposely does not have a #! as it is written to work with Bash and ZSH.
set -e


# Variables.
AZURITE_FOLDER=
AZURITE_PID=
MINIO_FOLDER=
MINIO_PID=


# Functions.
# Azurite Blob: http://127.0.0.1:10000.
# Azurite Queue: http://127.0.0.1:10001.
# Azurite Table: http://127.0.0.1:10002.
start_azurite() {
    print_arrow_message "Start Azurite"
    AZURITE_FOLDER=$(mktemp -d)
    azurite -l "$AZURITE_FOLDER" --silent --skipApiVersionCheck &
    AZURITE_PID=$!
    sleep 1 # Simple way to ensure Azurite has time to start.
    az storage container create -n modelardb --connection-string 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;'
    echo
}

stop_azurite() {
    print_arrow_message "Stop Azurite"
    if [ ! -z "$AZURITE_FOLDER" ]
    then
	kill "$AZURITE_PID"
	sleep 1 # Simple way to ensure Azurite has time to write.
	rm -rf "$AZURITE_FOLDER"
    fi
    echo
}

# MinIO API: http://127.0.0.1:9000.
# MinIO WebUI: http://127.0.0.1:{random}.
start_minio() {
    print_arrow_message "Start MinIO"
    MINIO_FOLDER=$(mktemp -d)
    minio server "$MINIO_FOLDER" &
    MINIO_PID=$!
    sleep 1 # Simple way to ensure MinIO has time to start.
    mcli alias set local http://127.0.0.1:9000 minioadmin minioadmin
    mcli mb local/modelardb
    echo
}

stop_minio() {
    print_arrow_message "Stop MinIO"
    if [ ! -z "$MINIO_FOLDER" ]
    then
	kill "$MINIO_PID"
	sleep 1 # Simple way to ensure MinIO has time to write.
	rm -rf "$MINIO_FOLDER"
    fi
    echo
}

print_error_message() {
    printf "\033[1;31mERROR: \033[39m%s\n" "$1"
}

print_arrow_message() {
  printf "\033[1;92m==> \033[39m%s \033[m\n" "$1"
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

trap 'stop_azurite; stop_minio' EXIT

start_azurite
start_minio

print_arrow_message "Waiting"
echo "Started object stores, press enter to stop."
read
