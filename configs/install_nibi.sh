#!/usr/bin/env zsh
# Source this installer from zsh on a Nibi login node.
# The account comes from the existing SLURM_ACCOUNT environment variable.
(
    set -euo pipefail

    HQ_INSTALL_DIR="/project/$SLURM_ACCOUNT/tools/hyperqueue"
    mkdir -p "$HQ_INSTALL_DIR/0.26.2"
    mkdir -p "$HOME/.bashrc.d"
    mkdir -p "$HOME/hyperqueue"
    mkdir -p "$HOME/logs"

    curl -fL --retry 3 \
        -o "$HQ_INSTALL_DIR/release.json" \
        'https://api.github.com/repos/jaredfischbach/hyperqueue/releases/latest'
    HQ_RELEASE_TAG=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["tag_name"])' \
        "$HQ_INSTALL_DIR/release.json")
    curl -fL --retry 3 \
        -o "$HQ_INSTALL_DIR/0.26.2/hq-fork.tar.gz" \
        "https://github.com/jaredfischbach/hyperqueue/releases/download/$HQ_RELEASE_TAG/hq-$HQ_RELEASE_TAG-linux-x64.tar.gz"
    tar -xzf "$HQ_INSTALL_DIR/0.26.2/hq-fork.tar.gz" \
        -C "$HQ_INSTALL_DIR/0.26.2" hq

    curl -fL --retry 3 \
        -o "$HQ_INSTALL_DIR/nibi.sh" \
        "https://raw.githubusercontent.com/jaredfischbach/hyperqueue/$HQ_RELEASE_TAG/configs/nibi.sh"

    cat > "$HOME/.bashrc.d/hyperqueue.sh" <<EOF
export PATH="$HQ_INSTALL_DIR/0.26.2:\$PATH"
export HQ_JOURNAL_DIR="\$HOME/hyperqueue/journal"
export HQ_SERVER_DIR="\$HOME/hyperqueue/server"
EOF

    curl -fL --retry 3 \
        "https://raw.githubusercontent.com/jaredfischbach/hyperqueue/$HQ_RELEASE_TAG/configs/hyperqueue_server.sh" | \
        sed -e "s|\$SLURM_ACCOUNT|$SLURM_ACCOUNT|g" \
            -e "s|\$HOME|$HOME|g" > "$HOME/hyperqueue/hyperqueue_server.sh"
    chmod +x "$HQ_INSTALL_DIR/0.26.2/hq" "$HQ_INSTALL_DIR/nibi.sh" \
        "$HOME/hyperqueue/hyperqueue_server.sh"

    # Replace previously installed launcher functions, including the old hqstart name.
    python3 - "$HOME/.zshrc" <<'PY'
import re, sys
from pathlib import Path
path = Path(sys.argv[1])
text = path.read_text() if path.exists() else ''
text = re.sub(r'(?m)^hqstart(?:_shell|_tmux|_nohup)?\(\) \{\n.*?^\}\n?', '', text, flags=re.S)
path.write_text(text + '''
hqstart_shell() {
    sbatch "$HOME/hyperqueue/hyperqueue_server.sh"
}

hqstart_tmux() {
    tmux new-session -d -s hyperqueue bash "$HOME/hyperqueue/hyperqueue_server.sh"
}

hqstart_nohup() {
    nohup bash "$HOME/hyperqueue/hyperqueue_server.sh" > "$HOME/logs/hyperqueue_server_nohup.log" 2>&1 < /dev/null &
}
''')
PY
) && {
    unset -f hqstart 2>/dev/null || true
    source "$HOME/.bashrc.d/hyperqueue.sh" &&
    source "$HOME/.zshrc" &&
    hq --version
}
