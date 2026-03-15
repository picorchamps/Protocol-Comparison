#!/usr/bin/env bash
# gen_certs.sh — Generate self-signed TLS certificates for the QUIC scenario.
#
# Output layout (relative to repo root):
#   certs/server.crt   — server certificate (PEM)
#   certs/server.key   — private key (PEM)
#   certs/ca.crt       — CA certificate trusted by the sender (= server.crt for self-signed)
#
# Both the sender and receiver containers mount ./certs as /certs.
# The receiver reads server.crt + server.key; the sender reads ca.crt.
#
# Usage:
#   bash scripts/gen_certs.sh            # generate if absent / expired
#   bash scripts/gen_certs.sh --force    # regenerate unconditionally

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CERTS_DIR="$REPO_ROOT/certs"
CERT="$CERTS_DIR/server.crt"
KEY="$CERTS_DIR/server.key"
CA="$CERTS_DIR/ca.crt"
DAYS=365

mkdir -p "$CERTS_DIR"

# Check whether we should skip generation
if [[ "${1:-}" != "--force" ]] && [[ -f "$CERT" ]]; then
    if openssl x509 -checkend 86400 -noout -in "$CERT" 2>/dev/null; then
        echo "[gen_certs] Certificates already exist and are valid. Use --force to regenerate."
        exit 0
    else
        echo "[gen_certs] Existing certificate is expired or expiring soon — regenerating."
    fi
fi

echo "[gen_certs] Generating ${DAYS}-day self-signed certificate in $CERTS_DIR …"

# Write a temporary OpenSSL config with the required Subject Alternative Names.
TMPCONF="$(mktemp /tmp/pico_openssl_XXXXXX.cnf)"
trap 'rm -f "$TMPCONF"' EXIT

cat > "$TMPCONF" <<EOF
[req]
default_bits       = 2048
prompt             = no
default_md         = sha256
distinguished_name = dn
x509_extensions    = v3_req

[dn]
CN = pico-receiver

[v3_req]
subjectAltName = @alt_names
basicConstraints = CA:TRUE

[alt_names]
DNS.1 = receiver
DNS.2 = localhost
IP.1  = 127.0.0.1
EOF

openssl req -x509 \
    -newkey rsa:2048 \
    -keyout "$KEY" \
    -out   "$CERT" \
    -days  "$DAYS" \
    -nodes \
    -config "$TMPCONF"

# The sender only needs to trust the CA (which is the self-signed server cert itself).
cp "$CERT" "$CA"

echo "[gen_certs] Done."
echo "  $CERT"
echo "  $KEY"
echo "  $CA"
