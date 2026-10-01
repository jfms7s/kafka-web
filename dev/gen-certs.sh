#!/usr/bin/env bash
# Generate the throwaway TLS material for the dev Kafka stack (dev/compose.yaml) into dev/certs/:
#   ca.pem          CA certificate (PEM)
#   broker.pem      broker keystore: unencrypted PKCS#8 key + broker cert + CA cert (PEM)
#   truststore.jks  JKS truststore holding the CA, password "changeit" (written with pyjks, no Java)
# The broker cert has SAN DNS:localhost and IP:127.0.0.1. Idempotent: refuses to overwrite an
# existing dev/certs/ unless --force is given.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
certs="$here/certs"
truststore_password="changeit"

case "${1:-}" in
  "" ) force=0 ;;
  --force) force=1 ;;
  *) echo "usage: $0 [--force]" >&2; exit 2 ;;
esac

if [[ -e "$certs" && $force -eq 0 ]]; then
  echo "$certs already exists; use --force to regenerate" >&2
  exit 1
fi

rm -rf "$certs"
mkdir -p "$certs"
cd "$certs"

# CA
openssl req -x509 -newkey rsa:2048 -nodes -days 825 -sha256 \
  -subj "/CN=kafka-web dev CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -keyout ca.key -out ca.pem 2>/dev/null

# Broker certificate signed by the CA
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout broker.key -out broker.csr 2>/dev/null
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n' > broker.ext
openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -days 825 -sha256 -extfile broker.ext -out broker.crt 2>/dev/null

# Keystore for the broker: PKCS#8 key, then the leaf, then the CA
openssl pkcs8 -topk8 -nocrypt -in broker.key -out broker.key.pk8
cat broker.key.pk8 broker.crt ca.pem > broker.pem

# JKS truststore with the CA, written with pyjks from the backend environment (no Java needed)
uv run --project "$here/../backend" python - "$certs/ca.pem" "$certs/truststore.jks" "$truststore_password" <<'PY'
import sys

import jks
from cryptography import x509
from cryptography.hazmat.primitives import serialization

ca_path, out_path, password = sys.argv[1:4]
with open(ca_path, "rb") as f:
    ca = x509.load_pem_x509_certificate(f.read())
entry = jks.TrustedCertEntry.new("ca", ca.public_bytes(serialization.Encoding.DER))
jks.KeyStore.new("jks", [entry]).save(out_path, password)
PY

# Only the files the stack and the app need stay; the broker key is a throwaway dev key, so the
# keystore is world-readable (the container user must be able to read the bind mount).
rm -f ca.key ca.srl broker.key broker.key.pk8 broker.csr broker.crt broker.ext
chmod 0644 ca.pem broker.pem truststore.jks
chmod 0755 "$certs"
echo "wrote $certs: ca.pem broker.pem truststore.jks (password: $truststore_password)"
