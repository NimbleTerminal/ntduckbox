#!/bin/sh
# Build on the target OS/architecture: the native engine requires CGO.
# NTDUCKBOX_ENGINE selects duckdb (default) or haybarn.
set -eu
version=${NTDUCKBOX_VERSION:?Set NTDUCKBOX_VERSION to vX.Y.Z}
if ! printf '%s\n' "$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
    printf '%s\n' 'Release version must have the form vX.Y.Z.' >&2
    exit 1
fi
platform=$(go env GOOS)
arch=$(go env GOARCH)
case "$platform/$arch" in
    linux/amd64|darwin/arm64) ;;
    *) printf 'Unsupported release target: %s/%s\n' "$platform" "$arch" >&2; exit 1 ;;
esac
engine=${NTDUCKBOX_ENGINE:-duckdb}
case "$engine" in
    duckdb) tags='' ; suffix='' ;;
    haybarn) tags='haybarn' ; suffix='_haybarn' ;;
    *) printf 'Unsupported engine: %s (use duckdb or haybarn)\n' "$engine" >&2; exit 1 ;;
esac
archive="ntduckbox_${version}_${platform}_${arch}${suffix}"
mkdir -p "dist/$archive"
CGO_ENABLED=1 GOWORK=off go build -mod=readonly -trimpath -tags "$tags" -ldflags "-X main.version=$version" -o "dist/$archive/ntduckbox" ./cmd/ntduckbox
cp LICENSE README.md CHANGELOG.md "dist/$archive/"
tar -czf "dist/$archive.tar.gz" -C dist "$archive"
printf 'Created dist/%s.tar.gz\n' "$archive"
