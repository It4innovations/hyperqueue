# HyperQueue for Nibi

## Install on Nibi

Paste this block into a zsh terminal on a Nibi login node. It uses the existing `$SLURM_ACCOUNT` from your login environment for the project paths and launcher account. Requires `curl`, `unzip`, `tar`, Python 3, and the SLURM client commands.

This installs this fork's Linux x86-64 build, the Nibi allocation config, and a shared-server launcher. The server requests 2 CPUs and 4096M, stops after 30 minutes without waiting or running jobs, and flushes its journal and exports timestamped HTML before stopping. It also shuts down five minutes before its seven-day walltime expires, including when jobs are still active. The journal is retained without pruning.

```bash
(
    set -euo pipefail

    # Use SLURM_ACCOUNT from the login environment.
    HQ_INSTALL_DIR="/project/$SLURM_ACCOUNT/tools/hyperqueue"

    mkdir -p "$HQ_INSTALL_DIR/0.26.2"
    mkdir -p "$HOME/.bashrc.d"
    mkdir -p "$HOME/hyperqueue"
    mkdir -p "$HOME/logs"

    curl -fL --retry 3 \
        -o "$HQ_INSTALL_DIR/0.26.2/hq-fork.zip" \
        'https://nightly.link/jaredfischbach/hyperqueue/workflows/build.yml/main/archive-x64.zip'
    unzip -o "$HQ_INSTALL_DIR/0.26.2/hq-fork.zip" -d "$HQ_INSTALL_DIR/0.26.2"
    tar -xzf "$HQ_INSTALL_DIR/0.26.2/hq-dev-linux-x64.tar.gz" \
        -C "$HQ_INSTALL_DIR/0.26.2" hq

    curl -fL --retry 3 \
        -o "$HQ_INSTALL_DIR/nibi.sh" \
        'https://raw.githubusercontent.com/jaredfischbach/hyperqueue/main/configs/nibi.sh'

    cat > "$HOME/.bashrc.d/hyperqueue.sh" <<EOF
export PATH="$HQ_INSTALL_DIR/0.26.2:\$PATH"
export HQ_JOURNAL_DIR="\$HOME/hyperqueue/journal"
export HQ_SERVER_DIR="\$HOME/hyperqueue/server"
EOF

    curl -fL --retry 3 \
        'https://raw.githubusercontent.com/jaredfischbach/hyperqueue/main/configs/hyperqueue_server.sh' | \
        sed -e "s|\$SLURM_ACCOUNT|$SLURM_ACCOUNT|g" \
            -e "s|\$HOME|$HOME|g" > "$HOME/hyperqueue/hyperqueue_server.sh"
    chmod +x "$HQ_INSTALL_DIR/0.26.2/hq" "$HQ_INSTALL_DIR/nibi.sh" \
        "$HOME/hyperqueue/hyperqueue_server.sh"

    echo '
hqstart() {
    sbatch "$HOME/hyperqueue/hyperqueue_server.sh"
}
' >> "$HOME/.zshrc"
) &&
source "$HOME/.bashrc.d/hyperqueue.sh" &&
source "$HOME/.zshrc" &&
hq --version

# Run hqstart when you are ready to submit the shared HQ server job.
```

The server logs are saved under `$HOME/logs/`, and journal reports under `$HQ_JOURNAL_DIR/reports/`. The Nibi allocation definitions are in `/project/$SLURM_ACCOUNT/tools/hyperqueue/nibi.sh`.
