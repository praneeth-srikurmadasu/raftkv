# Shared helpers for the RaftKV shell scripts (Linux, macOS, Git Bash). Source it.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT/data/pids"
LOG_DIR="$ROOT/logs"
SETTINGS="$ROOT/data/cluster.env"

# The cluster's shape, saved by run-cluster.sh so the other scripts agree with it:
# NODES = initial voters (n1..nN), SPARES = extra servers that can join later, BASE_PORT = port of n1.
load_settings() {
  NODES=3; SPARES=0; BASE_PORT=7001
  # shellcheck disable=SC1090
  [[ -f "$SETTINGS" ]] && source "$SETTINGS"
  TOTAL=$((NODES + SPARES))
}

save_settings() { # save_settings <nodes> <spares> <base-port>
  mkdir -p "$(dirname "$SETTINGS")"
  printf 'NODES=%s\nSPARES=%s\nBASE_PORT=%s\n' "$1" "$2" "$3" >"$SETTINGS"
}

peers() { # peers <count> <base-port>
  local out="" i
  for ((i = 1; i <= $1; i++)); do
    out="${out:+$out,}$i=127.0.0.1:$(($2 + i - 1))"
  done
  echo "$out"
}

voters() { # voters <count>
  local out="" i
  for ((i = 1; i <= $1; i++)); do out="${out:+$out,}$i"; done
  echo "$out"
}

node_pid() { # prints the pid if node $1 is running
  local f="$PID_DIR/n$1.pid"
  [[ -f "$f" ]] || return 1
  local p; p="$(cat "$f")"
  kill -0 "$p" 2>/dev/null && echo "$p"
}

build() {
  echo "Compiling (mvnw -q -DskipTests compile)..."
  (cd "$ROOT" && ./mvnw -q -DskipTests compile)
}

start_node() { # start_node <id>   (uses the saved settings)
  load_settings
  if node_pid "$1" >/dev/null; then echo "n$1 is already running"; return; fi
  mkdir -p "$PID_DIR" "$LOG_DIR"
  local list; list="$(peers "$TOTAL" "$BASE_PORT")"
  local vs; vs="$(voters "$NODES")"
  (
    cd "$ROOT" || exit 1
    # A simple command in the background, so $! is java's own pid (what kill-node must kill).
    nohup java -cp target/classes dev.raftkv.Main --id "$1" --peers "$list" --voters "$vs" --data "data/n$1" \
      </dev/null >>"$LOG_DIR/n$1.log" 2>&1 &
    echo $! >"$PID_DIR/n$1.pid"
  )
  local role=voter; [[ "$1" -gt "$NODES" ]] && role=spare
  echo "started n$1 (pid $(cat "$PID_DIR/n$1.pid"), $role) on port $((BASE_PORT + $1 - 1)), log: logs/n$1.log"
}
