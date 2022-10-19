#!/bin/bash

set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
mkdir data

# Update, upgrade and install wget, unzip and miller
apt-get update
apt-get -y upgrade
apt-get -y install --no-install-recommends wget unzip
apt-get clean
rm -rf /var/lib/apt/lists/*

wget --no-check-certificate https://github.com/duckdb/duckdb/releases/download/v0.5.1/duckdb_cli-linux-amd64.zip
wget --no-check-certificate https://github.com/johnkerl/miller/releases/download/v6.4.0/miller-6.4.0-linux-amd64.deb
mkdir -p bin
unzip duckdb_cli-linux-amd64.zip -d bin
DEBIAN_FRONTEND=noninteractive dpkg -i miller-6.4.0-linux-amd64.deb
cd bin
chmod +x duckdb
cd ..
export PATH=/app/bin:$PATH
