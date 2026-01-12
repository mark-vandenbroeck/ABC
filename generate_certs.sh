#!/bin/bash
set -e

# Directory for certs
CERT_DIR="stats_certs"
mkdir -p $CERT_DIR
cd $CERT_DIR

echo "Generating certificates in $(pwd)..."

# 1. Generate Root CA
# -------------------
echo "Generating Root CA..."
openssl req -new -x509 -days 3650 -nodes -text -out root.crt \
    -keyout root.key -subj "/CN=ABC_Root_CA"
chmod 600 root.key

# 2. Generate Server Certificate
# ------------------------------
echo "Generating Server Certificate..."
openssl req -new -nodes -text -out server.csr \
    -keyout server.key -subj "/CN=localhost"
chmod 600 server.key

# Sign with Root CA
openssl x509 -req -in server.csr -text -days 3650 \
    -CA root.crt -CAkey root.key -CAcreateserial \
    -out server.crt

# 3. Generate Client Certificate (for user 'mark')
# ------------------------------------------------
echo "Generating Client Certificate for user 'mark'..."
openssl req -new -nodes -text -out client.csr \
    -keyout client.key -subj "/CN=mark"
chmod 600 client.key

# Sign with Root CA
openssl x509 -req -in client.csr -text -days 3650 \
    -CA root.crt -CAkey root.key -CAcreateserial \
    -out client.crt

echo "------------------------------------------------"
echo "Certificates generated successfully!"
echo "Files created:"
ls -1 *.crt *.key
echo "------------------------------------------------"
