#!/usr/bin/env bash
# =============================================================================
# FlyMQ Uninstaller
# Copyright (c) 2026 Firefly Software Solutions Inc.
# =============================================================================

set -euo pipefail

readonly SCRIPT_VERSION="1.26.1"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Detected system info
OS=""
ARCH=""

# =============================================================================
# Default Paths
# =============================================================================

# Will be detected/prompted
PREFIX=""
CONFIG_DIR=""
DATA_DIR=""
SYSTEMD_SERVICE=""
LAUNCHD_PLIST=""

# Options
AUTO_CONFIRM=false
REMOVE_DATA=false
REMOVE_CONFIG=false
DRY_RUN=false

# =============================================================================
# Colors and Formatting
# =============================================================================

if [[ -t 1 ]] && [[ -z "${NO_COLOR:-}" ]]; then
    readonly COLOR_ENABLED=true
else
    readonly COLOR_ENABLED=false
fi

if [[ "$COLOR_ENABLED" == true ]]; then
    readonly RESET='\033[0m'
    readonly BOLD='\033[1m'
    readonly DIM='\033[2m'
    readonly RED='\033[31m'
    readonly GREEN='\033[32m'
    readonly YELLOW='\033[33m'
    readonly CYAN='\033[36m'
else
    readonly RESET='' BOLD='' DIM='' RED='' GREEN='' YELLOW='' CYAN=''
fi

readonly ICON_SUCCESS="✓"
readonly ICON_ERROR="✗"
readonly ICON_WARNING="⚠"
readonly ICON_INFO="ℹ"
readonly ICON_ARROW="→"

# =============================================================================
# Output Functions
# =============================================================================

print_success() { echo -e "${GREEN}${ICON_SUCCESS}${RESET} $1"; }
print_error() { echo -e "${RED}${ICON_ERROR}${RESET} ${RED}$1${RESET}" >&2; }
print_warning() { echo -e "${YELLOW}${ICON_WARNING}${RESET} $1"; }
print_info() { echo -e "${CYAN}${ICON_INFO}${RESET} $1"; }
print_step() { echo -e "\n${CYAN}${BOLD}==>${RESET} ${BOLD}$1${RESET}"; }
print_section() { echo -e "\n  ${BOLD}${CYAN}━━━ $1 ━━━${RESET}\n"; }

# =============================================================================
# Banner
# =============================================================================

print_banner() {
    local banner_file="${SCRIPT_DIR}/internal/banner/banner.txt"
    echo ""
    if [[ -f "$banner_file" ]]; then
        while IFS= read -r line; do
            # Output line with colors, then reset (avoids backslash escaping RESET)
            echo -ne "${CYAN}${BOLD}"
            echo -e "${line}"
            echo -ne "${RESET}"
        done < "$banner_file"
    else
        echo -e "${CYAN}${BOLD}  FlyMQ${RESET}"
    fi
    echo ""
    echo -e "  ${BOLD}Uninstaller${RESET}"
    echo -e "  ${DIM}Version ${SCRIPT_VERSION}${RESET}"
    echo ""
    echo -e "  ${DIM}Copyright (c) 2026 Firefly Software Solutions Inc.${RESET}"
    echo -e "  ${DIM}Licensed under the Apache License 2.0${RESET}"
    echo ""
}

# =============================================================================
# Utility Functions
# =============================================================================

prompt_yes_no() {
    local prompt="$1"
    local default="$2"
    local result

    if [[ "$AUTO_CONFIRM" == true ]]; then
        [[ "$default" == "y" ]]
        return
    fi

    while true; do
        if [[ "$default" == "y" ]]; then
            echo -en "  ${prompt} [${DIM}Y/n${RESET}]: " >&2
        else
            echo -en "  ${prompt} [${DIM}y/N${RESET}]: " >&2
        fi
        read -r result
        result="${result:-$default}"
        
        if [[ "$result" =~ ^[YyNn]$ ]] || [[ -z "$result" ]]; then
            [[ "$result" =~ ^[Yy] ]] && return 0
            return 1
        fi
        print_warning "Please answer 'y' or 'n'"
    done
}

prompt_value() {
    local prompt="$1"
    local default="$2"
    local result

    echo -en "  ${prompt} [${DIM}${default}${RESET}]: " >&2
    read -r result
    result="${result:-$default}"
    echo "$result"
}

detect_system() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    # Detect Windows environments
    case "$OS" in
        mingw*|msys*|cygwin*)
            OS="windows"
            if grep -qEi "(Microsoft|WSL)" /proc/version 2>/dev/null; then
                OS="linux"
            fi
            ;;
    esac

    case "$ARCH" in
        x86_64|amd64) ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        i386|i686) ARCH="386" ;;
    esac
}

detect_installation() {
    print_step "Detecting FlyMQ installation"
    echo ""

    # Check common installation locations
    local found=false

    # Platform-specific detection
    if [[ "$OS" == "windows" ]]; then
        # Windows paths
        local win_prefix="$HOME/AppData/Local/FlyMQ"
        if [[ -f "$win_prefix/bin/flymq.exe" ]]; then
            PREFIX="$win_prefix"
            CONFIG_DIR="$win_prefix/config"
            DATA_DIR="$win_prefix/data"
            found=true
        fi
    else
        # Unix-like systems (Linux/macOS)
        # Check /usr/local/bin (root install)
        if [[ -f "/usr/local/bin/flymq" ]]; then
            PREFIX="/usr/local"
            found=true
        fi

        # Check ~/.local/bin (user install)
        if [[ -f "$HOME/.local/bin/flymq" ]]; then
            PREFIX="$HOME/.local"
            found=true
        fi

        # Detect config directory
        if [[ -d "/etc/flymq" ]]; then
            CONFIG_DIR="/etc/flymq"
        elif [[ -d "$HOME/.config/flymq" ]]; then
            CONFIG_DIR="$HOME/.config/flymq"
        fi

        # Detect data directory
        if [[ -d "/var/lib/flymq" ]]; then
            DATA_DIR="/var/lib/flymq"
        elif [[ -d "$HOME/.local/share/flymq" ]]; then
            DATA_DIR="$HOME/.local/share/flymq"
        fi

        # Detect systemd service (Linux only)
        if [[ "$OS" == "linux" ]]; then
            if [[ -f "/etc/systemd/system/flymq.service" ]]; then
                SYSTEMD_SERVICE="flymq"
            elif [[ -f "/etc/systemd/system/flymq-cluster@.service" ]]; then
                SYSTEMD_SERVICE="flymq-cluster@"
            fi
        fi

        # Detect launchd plist (macOS only)
        if [[ "$OS" == "darwin" ]]; then
            if [[ -f "$HOME/Library/LaunchAgents/com.firefly.flymq.plist" ]]; then
                LAUNCHD_PLIST="$HOME/Library/LaunchAgents/com.firefly.flymq.plist"
            fi
        fi
    fi

    if [[ "$found" == true ]]; then
        print_success "Found FlyMQ installation"
        print_info "Platform: ${CYAN}$OS/$ARCH${RESET}"
        return 0
    else
        return 1
    fi
}

show_installation_summary() {
    echo ""
    echo -e "  ${BOLD}Detected Installation:${RESET}"
    echo ""

    if [[ -n "$PREFIX" ]]; then
        echo -e "  ${ICON_ARROW} Binaries:      ${CYAN}$PREFIX/bin/flymq${RESET}"
        echo -e "  ${ICON_ARROW}                ${CYAN}$PREFIX/bin/flymq-cli${RESET}"
    fi

    if [[ -n "$CONFIG_DIR" ]]; then
        echo -e "  ${ICON_ARROW} Configuration: ${CYAN}$CONFIG_DIR${RESET}"
    fi

    if [[ -n "$DATA_DIR" ]]; then
        echo -e "  ${ICON_ARROW} Data:          ${CYAN}$DATA_DIR${RESET}"
    fi

    if [[ -n "$SYSTEMD_SERVICE" ]]; then
        echo -e "  ${ICON_ARROW} Systemd:       ${CYAN}$SYSTEMD_SERVICE${RESET}"
    fi

    if [[ -n "$LAUNCHD_PLIST" ]]; then
        echo -e "  ${ICON_ARROW} Launch Agent:  ${CYAN}$LAUNCHD_PLIST${RESET}"
    fi

    echo ""
}

stop_running_processes() {
    # Stop any running FlyMQ processes (not managed by systemd/launchd)
    local process_count=0
    
    if [[ "$OS" == "windows" ]]; then
        process_count=$(tasklist 2>/dev/null | grep -i "flymq" | wc -l || true)
    else
        # Use pgrep but handle the case where it returns 1 (no processes found)
        local pids
        pids=$(pgrep -f "flymq" 2>/dev/null || true)
        if [[ -n "$pids" ]]; then
            process_count=$(echo "$pids" | wc -l)
        fi
    fi
    
    if [[ $process_count -gt 0 ]]; then
        print_step "Stopping running FlyMQ processes"
        echo ""
        
        if [[ "$DRY_RUN" == true ]]; then
            print_info "[DRY RUN] Would stop $process_count FlyMQ process(es)"
        else
            if [[ "$OS" == "windows" ]]; then
                # Windows: use taskkill
                taskkill //F //IM flymq.exe 2>/dev/null || true
                taskkill //F //IM flymq-cli.exe 2>/dev/null || true
            else
                # Unix-like: use pkill
                pkill -TERM -f "flymq" 2>/dev/null || true
                sleep 2
                # Force kill if still running
                pkill -KILL -f "flymq" 2>/dev/null || true
            fi
            print_success "Stopped $process_count FlyMQ process(es)"
        fi
    fi
}

interactive_uninstall() {
    print_section "Uninstall Options"

    # Ask about data removal
    if [[ -n "$DATA_DIR" ]] && [[ -d "$DATA_DIR" ]]; then
        echo -e "  ${BOLD}Data Directory${RESET}"
        echo -e "  ${DIM}Contains message data, topics, and indexes${RESET}"
        echo -e "  ${DIM}Location: $DATA_DIR${RESET}"
        if prompt_yes_no "Remove data directory (DESTRUCTIVE)" "n"; then
            REMOVE_DATA=true
            print_warning "Data directory will be removed"
        else
            print_info "Data directory will be preserved"
        fi
        echo ""
    fi

    # Ask about config removal
    if [[ -n "$CONFIG_DIR" ]] && [[ -d "$CONFIG_DIR" ]]; then
        echo -e "  ${BOLD}Configuration Directory${RESET}"
        echo -e "  ${DIM}Contains configuration files and certificates${RESET}"
        echo -e "  ${DIM}Location: $CONFIG_DIR${RESET}"
        if prompt_yes_no "Remove configuration directory" "n"; then
            REMOVE_CONFIG=true
            print_warning "Configuration directory will be removed"
        else
            print_info "Configuration directory will be preserved"
        fi
        echo ""
    fi
}

stop_services() {
    # Linux systemd
    if [[ "$OS" == "linux" ]] && [[ -n "$SYSTEMD_SERVICE" ]] && command -v systemctl &> /dev/null; then
        print_step "Stopping FlyMQ services"
        echo ""

        # Stop the service
        if systemctl is-active --quiet "$SYSTEMD_SERVICE" 2>/dev/null; then
            if [[ "$DRY_RUN" == true ]]; then
                print_info "[DRY RUN] Would stop: $SYSTEMD_SERVICE"
            else
                sudo systemctl stop "$SYSTEMD_SERVICE" 2>/dev/null || true
                print_success "Stopped $SYSTEMD_SERVICE"
            fi
        fi

        # Disable the service
        if systemctl is-enabled --quiet "$SYSTEMD_SERVICE" 2>/dev/null; then
            if [[ "$DRY_RUN" == true ]]; then
                print_info "[DRY RUN] Would disable: $SYSTEMD_SERVICE"
            else
                sudo systemctl disable "$SYSTEMD_SERVICE" 2>/dev/null || true
                print_success "Disabled $SYSTEMD_SERVICE"
            fi
        fi
    
    # macOS launchd
    elif [[ "$OS" == "darwin" ]] && [[ -n "$LAUNCHD_PLIST" ]]; then
        print_step "Stopping FlyMQ Launch Agent"
        echo ""

        # Check if service is loaded (grep returns 1 if not found, which is ok)
        local is_loaded=false
        if launchctl list 2>/dev/null | grep -q "com.firefly.flymq" 2>/dev/null; then
            is_loaded=true
        fi
        
        if [[ "$is_loaded" == true ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                print_info "[DRY RUN] Would unload: $LAUNCHD_PLIST"
            else
                launchctl unload "$LAUNCHD_PLIST" 2>/dev/null || true
                print_success "Unloaded Launch Agent"
            fi
        fi
    fi
}

remove_binaries() {
    if [[ -z "$PREFIX" ]]; then
        return 0
    fi

    print_step "Removing binaries"
    echo ""

    local bin_dir="$PREFIX/bin"

    # Platform-specific binary names
    local server_bin="flymq"
    local cli_bin="flymq-cli"
    
    if [[ "$OS" == "windows" ]]; then
        server_bin="flymq.exe"
        cli_bin="flymq-cli.exe"
    fi

    if [[ -f "$bin_dir/$server_bin" ]]; then
        if [[ "$DRY_RUN" == true ]]; then
            print_info "[DRY RUN] Would remove: $bin_dir/$server_bin"
        else
            rm -f "$bin_dir/$server_bin"
            print_success "Removed $bin_dir/$server_bin"
        fi
    fi

    if [[ -f "$bin_dir/$cli_bin" ]]; then
        if [[ "$DRY_RUN" == true ]]; then
            print_info "[DRY RUN] Would remove: $bin_dir/$cli_bin"
        else
            rm -f "$bin_dir/$cli_bin"
            print_success "Removed $bin_dir/$cli_bin"
        fi
    fi
}

remove_system_services() {
    # Linux systemd
    if [[ "$OS" == "linux" ]] && [[ -n "$SYSTEMD_SERVICE" ]]; then
        print_step "Removing systemd service"
        echo ""

        if [[ -f "/etc/systemd/system/flymq.service" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                print_info "[DRY RUN] Would remove: /etc/systemd/system/flymq.service"
            else
                sudo rm -f /etc/systemd/system/flymq.service
                print_success "Removed flymq.service"
            fi
        fi

        if [[ -f "/etc/systemd/system/flymq-cluster@.service" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                print_info "[DRY RUN] Would remove: /etc/systemd/system/flymq-cluster@.service"
            else
                sudo rm -f /etc/systemd/system/flymq-cluster@.service
                print_success "Removed flymq-cluster@.service"
            fi
        fi

        if [[ "$DRY_RUN" != true ]] && command -v systemctl &> /dev/null; then
            sudo systemctl daemon-reload
            print_success "Reloaded systemd daemon"
        fi
    
    # macOS launchd
    elif [[ "$OS" == "darwin" ]] && [[ -n "$LAUNCHD_PLIST" ]]; then
        print_step "Removing Launch Agent"
        echo ""

        if [[ -f "$LAUNCHD_PLIST" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                print_info "[DRY RUN] Would remove: $LAUNCHD_PLIST"
            else
                rm -f "$LAUNCHD_PLIST"
                print_success "Removed $LAUNCHD_PLIST"
            fi
        fi
    fi
}

remove_data_directory() {
    if [[ "$REMOVE_DATA" != true ]] || [[ -z "$DATA_DIR" ]]; then
        return 0
    fi

    print_step "Removing data directory"
    echo ""

    if [[ -d "$DATA_DIR" ]]; then
        if [[ "$DRY_RUN" == true ]]; then
            print_info "[DRY RUN] Would remove: $DATA_DIR"
        else
            rm -rf "$DATA_DIR"
            print_success "Removed: ${CYAN}$DATA_DIR${RESET}"
        fi
    fi
}

remove_config_directory() {
    if [[ "$REMOVE_CONFIG" != true ]] || [[ -z "$CONFIG_DIR" ]]; then
        return 0
    fi

    print_step "Removing configuration directory"
    echo ""

    if [[ -d "$CONFIG_DIR" ]]; then
        if [[ "$DRY_RUN" == true ]]; then
            print_info "[DRY RUN] Would remove: $CONFIG_DIR"
        else
            rm -rf "$CONFIG_DIR"
            print_success "Removed: ${CYAN}$CONFIG_DIR${RESET}"
        fi
    fi
}

print_completion() {
    echo ""
    echo -e "${GREEN}${BOLD}════════════════════════════════════════════════════════════════${RESET}"
    if [[ "$DRY_RUN" == true ]]; then
        print_info "Dry run complete - no changes were made"
    else
        print_success "FlyMQ has been uninstalled"
    fi
    echo -e "${GREEN}${BOLD}════════════════════════════════════════════════════════════════${RESET}"
    echo ""

    if [[ "$REMOVE_DATA" != true ]] && [[ -n "$DATA_DIR" ]] && [[ -d "$DATA_DIR" ]]; then
        print_info "Data directory preserved: $DATA_DIR"
        echo -e "  ${DIM}To remove manually: rm -rf $DATA_DIR${RESET}"
    fi

    if [[ "$REMOVE_CONFIG" != true ]] && [[ -n "$CONFIG_DIR" ]] && [[ -d "$CONFIG_DIR" ]]; then
        print_info "Configuration preserved: $CONFIG_DIR"
        echo -e "  ${DIM}To remove manually: rm -rf $CONFIG_DIR${RESET}"
    fi

    echo ""
}

# =============================================================================
# Argument Parsing
# =============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --yes|-y)
                AUTO_CONFIRM=true
                shift
                ;;
            --remove-data)
                REMOVE_DATA=true
                shift
                ;;
            --remove-config)
                REMOVE_CONFIG=true
                shift
                ;;
            --remove-all)
                REMOVE_DATA=true
                REMOVE_CONFIG=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --prefix)
                PREFIX="$2"
                shift 2
                ;;
            --help|-h)
                print_banner
                echo "Usage: ./uninstall.sh [options]"
                echo ""
                echo "Options:"
                echo "  --yes, -y        Skip confirmation prompts"
                echo "  --remove-data    Remove data directory (messages, topics)"
                echo "  --remove-config  Remove configuration directory"
                echo "  --remove-all     Remove both data and configuration"
                echo "  --dry-run        Show what would be removed without removing"
                echo "  --prefix PATH    Specify installation prefix"
                echo "  --help, -h       Show this help"
                echo ""
                echo "Examples:"
                echo "  ./uninstall.sh                    # Interactive uninstall"
                echo "  ./uninstall.sh --yes              # Quick uninstall, keep data"
                echo "  ./uninstall.sh --yes --remove-all # Complete removal"
                echo "  ./uninstall.sh --dry-run          # Preview what would be removed"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
}

# =============================================================================
# Main
# =============================================================================

main() {
    parse_args "$@"

    print_banner
    
    # Detect system
    detect_system

    # Detect installation
    if ! detect_installation; then
        print_warning "No FlyMQ installation detected"
        echo ""
        echo -e "  ${DIM}If FlyMQ is installed in a custom location, use:${RESET}"
        echo -e "  ${CYAN}./uninstall.sh --prefix /path/to/installation${RESET}"
        echo ""
        exit 1
    fi

    show_installation_summary

    # Interactive mode
    if [[ "$AUTO_CONFIRM" != true ]]; then
        interactive_uninstall

        echo ""
        echo -e "  ${BOLD}${RED}━━━ Confirmation ━━━${RESET}"
        echo ""
        echo -e "  ${YELLOW}The following will be removed:${RESET}"
        local binary_suffix=""
        [[ "$OS" == "windows" ]] && binary_suffix=".exe"
        echo -e "  ${ICON_ARROW} Binaries: $PREFIX/bin/flymq$binary_suffix, $PREFIX/bin/flymq-cli$binary_suffix"
        [[ -n "$SYSTEMD_SERVICE" ]] && echo -e "  ${ICON_ARROW} Systemd service: $SYSTEMD_SERVICE"
        [[ -n "$LAUNCHD_PLIST" ]] && echo -e "  ${ICON_ARROW} Launch Agent: com.firefly.flymq"
        [[ "$REMOVE_DATA" == true ]] && echo -e "  ${ICON_ARROW} Data directory: $DATA_DIR"
        [[ "$REMOVE_CONFIG" == true ]] && echo -e "  ${ICON_ARROW} Config directory: $CONFIG_DIR"
        echo ""

        if ! prompt_yes_no "Proceed with uninstallation" "n"; then
            print_info "Uninstallation cancelled"
            exit 0
        fi
    fi

    echo ""
    print_step "Starting uninstallation process"

    # Perform uninstallation
    print_info "Stopping processes..."
    stop_running_processes
    print_info "Stopping services..."
    stop_services
    print_info "Removing binaries..."
    remove_binaries
    print_info "Removing system services..."
    remove_system_services
    print_info "Removing data directory..."
    remove_data_directory
    print_info "Removing config directory..."
    remove_config_directory

    print_completion
}

main "$@"

