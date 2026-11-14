# Shared helpers for the RaftKV shell scripts (Linux, macOS, Git Bash). Source it.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT/data/pids"
LOG_DIR="$ROOT/logs"

peers() { # peers <nodes> <base-port>
  local out="" i
  for ((i = 1; i <= $1; i++)); do
    out="${out:+$out,}$i=127.0.0.1:$(($2 + i - 1))"
  done
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

start_node() { # start_node <id> <nodes> <base-port>
  if node_pid "$1" >/dev/null; then echo "n$1 is already running"; return; fi
  mkdir -p "$PID_DIR" "$LOG_DIR"
  local list; list="$(peers "$2" "$3")"
  (
    cd "$ROOT" || exit 1
    # A simple command in the background, so $! is java's own pid (what kill-node must kill).
    nohup java -cp target/classes dev.raftkv.Main --id "$1" --peers "$list" --data "data/n$1" \
      </dev/null >>"$LOG_DIR/n$1.log" 2>&1 &
    echo $! >"$PID_DIR/n$1.pid"
  )
  echo "started n$1 (pid $(cat "$PID_DIR/n$1.pid")) on port $(($3 + $1 - 1)), log: logs/n$1.log"
}
