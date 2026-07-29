#!/bin/sh

set -eu

repository="gfreezy/swarmlite"
requested_runtime="${SWARMLITE_RUNTIME:-auto}"
requested_version="${SWARMLITE_VERSION:-latest}"
install_dir="/usr/local/bin"
data_dir="/var/lib/swarmlite"
runtime_config="/etc/swarmlite/runtime.env"
service_path="/etc/systemd/system/swarmlite.service"

log() {
    printf '%s\n' "swarmlite installer: $*"
}

fail() {
    printf '%s\n' "swarmlite installer: $*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Install Swarmlite and its platform integration.

Usage:
  install.sh [--runtime auto|docker|podman] [--version VERSION]

Defaults:
  --runtime auto    Reuse the previous or installed runtime; install Docker if none exists.
  --version latest  Install assets from the latest GitHub Release.

Linux installs a container runtime when necessary and configures systemd. macOS requires an
accessible Docker-compatible Unix socket, recommends OrbStack when none exists, and installs only
the CLI.
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --runtime)
            [ "$#" -ge 2 ] || fail "--runtime requires a value"
            requested_runtime="$2"
            shift 2
            ;;
        --version)
            [ "$#" -ge 2 ] || fail "--version requires a value"
            requested_version="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "unknown argument: $1"
            ;;
    esac
done

case "$requested_runtime" in
    auto|docker|podman) ;;
    *) fail "runtime must be auto, docker, or podman" ;;
esac

case "$requested_version" in
    ''|*[!A-Za-z0-9._-]*) fail "version contains unsupported characters" ;;
esac

command -v curl >/dev/null 2>&1 || fail "curl is required"

host_os="$(uname -s)"
case "$host_os" in
    Linux)
        [ "$(id -u)" -eq 0 ] || fail "run the Linux installer as root, for example: curl ... | sudo sh"
        command -v systemctl >/dev/null 2>&1 || fail "systemctl is required on Linux"
        [ -d /run/systemd/system ] || fail "systemd is not running"
        case "$(uname -m)" in
            x86_64|amd64) archive="swarmlite-linux-amd64.tar.gz" ;;
            aarch64|arm64) archive="swarmlite-linux-arm64.tar.gz" ;;
            *) fail "unsupported Linux architecture: $(uname -m)" ;;
        esac
        ;;
    Darwin)
        [ "$(id -u)" -ne 0 ] || fail "run the macOS installer without sudo so it can access your container runtime socket"
        case "$requested_runtime" in
            auto|docker) ;;
            podman) fail "the macOS installer expects a Docker-compatible socket" ;;
        esac
        case "$(uname -m)" in
            arm64|aarch64) archive="swarmlite-macos-arm64.tar.gz" ;;
            *) fail "the macOS installer currently supports Apple silicon only" ;;
        esac
        probe_macos_socket() {
            [ -S "$1" ] && curl --fail --silent --unix-socket "$1" \
                http://localhost/_ping >/dev/null 2>&1 && macos_docker_socket="$1"
        }
        macos_docker_socket=""
        if [ -n "${HOME:-}" ] && probe_macos_socket "$HOME/.orbstack/run/docker.sock"; then
            :
        elif probe_macos_socket /var/run/docker.sock; then
            :
        elif [ -n "${HOME:-}" ] && probe_macos_socket "$HOME/.docker/run/docker.sock"; then
            :
        else
            fail "no accessible Docker-compatible socket was found; install and start OrbStack: https://orbstack.dev/download"
        fi
        ;;
    *)
        fail "unsupported operating system: $host_os"
        ;;
esac

if [ "$requested_version" = "latest" ]; then
    release_url="https://github.com/$repository/releases/latest/download"
else
    release_url="https://github.com/$repository/releases/download/$requested_version"
fi

install_tmp="$(mktemp -d /tmp/swarmlite-install.XXXXXX)"
cleanup() {
    case "${install_tmp:-}" in
        /tmp/swarmlite-install.*) rm -rf -- "$install_tmp" ;;
    esac
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

download() {
    download_name="$1"
    log "downloading $download_name"
    curl --fail --location --silent --show-error --retry 3 \
        "$release_url/$download_name" --output "$install_tmp/$download_name"
}

download "$archive"
download "$archive.sha256"

if command -v sha256sum >/dev/null 2>&1; then
    (cd "$install_tmp" && sha256sum --check "$archive.sha256")
elif command -v shasum >/dev/null 2>&1; then
    (cd "$install_tmp" && shasum -a 256 --check "$archive.sha256")
else
    fail "sha256sum or shasum is required"
fi
tar -xzf "$install_tmp/$archive" -C "$install_tmp"
[ -x "$install_tmp/swarmlite" ] || fail "release archive does not contain an executable swarmlite"

if [ "$host_os" = "Darwin" ]; then
    log "installing the macOS CLI (Docker socket: $macos_docker_socket)"
    if [ -d "$install_dir" ] && [ -w "$install_dir" ]; then
        install -m 0755 "$install_tmp/swarmlite" "$install_dir/swarmlite"
    else
        command -v sudo >/dev/null 2>&1 || fail "sudo is required to install into $install_dir"
        sudo install -d -m 0755 "$install_dir"
        sudo install -m 0755 "$install_tmp/swarmlite" "$install_dir/swarmlite"
    fi
    log "installation complete; container runtime socket detected at $macos_docker_socket"
    "$install_dir/swarmlite" --help >/dev/null
    exit 0
fi

download "swarmlite.service"

if [ -r /etc/os-release ]; then
    # The values in os-release are distribution-controlled shell assignments.
    # shellcheck source=/dev/null
    . /etc/os-release
else
    fail "/etc/os-release is required to install a container runtime"
fi

install_docker_apt() {
    case "${ID:-}" in
        ubuntu)
            docker_repo_os="ubuntu"
            docker_suite="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"
            ;;
        debian)
            docker_repo_os="debian"
            docker_suite="${VERSION_CODENAME:-}"
            ;;
        *)
            fail "automatic Docker installation supports Ubuntu or Debian with apt"
            ;;
    esac
    [ -n "$docker_suite" ] || fail "could not determine the distribution codename"

    log "configuring Docker's official apt repository"
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl
    install -m 0755 -d /etc/apt/keyrings
    curl --fail --location --silent --show-error \
        "https://download.docker.com/linux/$docker_repo_os/gpg" \
        --output /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    cat > /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/$docker_repo_os
Suites: $docker_suite
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
}

install_docker_rpm() {
    case "${ID:-}" in
        fedora) docker_repo_os="fedora" ;;
        rhel) docker_repo_os="rhel" ;;
        centos) docker_repo_os="centos" ;;
        *)
            fail "automatic Docker installation supports Fedora, RHEL, or CentOS with dnf"
            ;;
    esac

    log "configuring Docker's official dnf repository"
    dnf -y install dnf-plugins-core
    dnf config-manager --add-repo "https://download.docker.com/linux/$docker_repo_os/docker-ce.repo"
    dnf -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
}

install_docker() {
    log "Docker is not installed; installing it"
    if command -v apt-get >/dev/null 2>&1; then
        install_docker_apt
    elif command -v dnf >/dev/null 2>&1; then
        install_docker_rpm
    else
        fail "automatic Docker installation does not support this distribution; install Docker manually or use --runtime podman"
    fi
}

install_podman() {
    log "Podman is not installed; installing it from the distribution repository"
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update
        DEBIAN_FRONTEND=noninteractive apt-get install -y podman
    elif command -v dnf >/dev/null 2>&1; then
        dnf -y install podman
    elif command -v yum >/dev/null 2>&1; then
        yum -y install podman
    elif command -v zypper >/dev/null 2>&1; then
        zypper --non-interactive install podman
    elif command -v pacman >/dev/null 2>&1; then
        pacman -Sy --noconfirm podman
    else
        fail "automatic Podman installation does not support this distribution"
    fi
}

docker_runtime_available() {
    command -v docker >/dev/null 2>&1 && systemctl cat docker.service >/dev/null 2>&1
}

podman_runtime_available() {
    command -v podman >/dev/null 2>&1 && systemctl cat podman.socket >/dev/null 2>&1
}

configured_runtime=""
if [ -r "$runtime_config" ]; then
    configured_runtime="$(sed -n 's/^SWARMLITE_RUNTIME=//p' "$runtime_config" | tail -n 1)"
    case "$configured_runtime" in
        docker|podman) ;;
        *) configured_runtime="" ;;
    esac
fi

if [ "$requested_runtime" = "auto" ]; then
    if [ "$configured_runtime" = "docker" ] && docker_runtime_available; then
        selected_runtime="docker"
    elif [ "$configured_runtime" = "podman" ] && podman_runtime_available; then
        selected_runtime="podman"
    elif docker_runtime_available; then
        selected_runtime="docker"
    elif podman_runtime_available; then
        selected_runtime="podman"
    elif command -v docker >/dev/null 2>&1; then
        selected_runtime="docker"
    elif command -v podman >/dev/null 2>&1; then
        selected_runtime="podman"
    else
        selected_runtime="docker"
    fi
else
    selected_runtime="$requested_runtime"
fi

case "$selected_runtime" in
    docker)
        if ! docker_runtime_available; then
            install_docker
        fi
        systemctl enable --now docker.service
        docker info >/dev/null 2>&1 || fail "Docker is installed but its daemon is not responding"
        runtime_socket="/var/run/docker.sock"
        ;;
    podman)
        if ! podman_runtime_available; then
            install_podman
        fi
        systemctl enable --now podman.socket
        runtime_socket="/run/podman/podman.sock"
        socket_attempt=0
        while [ ! -S "$runtime_socket" ] && [ "$socket_attempt" -lt 10 ]; do
            socket_attempt=$((socket_attempt + 1))
            sleep 1
        done
        [ -S "$runtime_socket" ] || fail "Podman API socket was not created at $runtime_socket"
        curl --fail --silent --show-error --unix-socket "$runtime_socket" \
            http://localhost/_ping >/dev/null || fail "Podman Docker-compatible API is not responding"
        ;;
esac

log "installing the Swarmlite CLI"
install -d -m 0755 "$install_dir"
install -m 0755 "$install_tmp/swarmlite" "$install_dir/swarmlite"
install -d -m 0700 "$data_dir"
install -d -m 0755 /etc/swarmlite
cat > "$runtime_config" <<EOF
# Managed by the Swarmlite installer.
SWARMLITE_RUNTIME=$selected_runtime
SWARMLITE_RUNTIME_SOCKET=$runtime_socket
EOF
chmod 0644 "$runtime_config"
install -m 0644 "$install_tmp/swarmlite.service" "$service_path"
systemctl daemon-reload

if [ -f "$data_dir/local.redb" ]; then
    log "existing node state found; enabling and restarting Swarmlite"
    systemctl enable swarmlite.service
    systemctl restart swarmlite.service
else
    log "installation complete; runtime: $selected_runtime"
    printf '\n%s\n' "Initialize or join this node, then start the service:"
    printf '  sudo swarmlite --data-dir %s init --mode standalone --runtime %s --runtime-socket %s\n' \
        "$data_dir" "$selected_runtime" "$runtime_socket"
    printf '  # or: sudo swarmlite --data-dir %s join <controller-url> --token <token> --runtime %s --runtime-socket %s\n' \
        "$data_dir" "$selected_runtime" "$runtime_socket"
    printf '  sudo systemctl enable --now swarmlite\n'
fi
