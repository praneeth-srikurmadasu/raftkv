package dev.raftkv.server;

import java.util.List;
import java.util.stream.Collectors;

/** The single static page of the live dashboard (SPEC §8), served by {@link MetricsServer}. */
final class Dashboard {

    private Dashboard() {
    }

    static String page(List<String> nodeUrls) {
        String urls = nodeUrls.stream().map(u -> "\"" + u.replace("\"", "") + "\"").collect(Collectors.joining(","));
        return TEMPLATE.replace("/*NODES*/", urls);
    }

    private static final String TEMPLATE = """
            <!DOCTYPE html>
            <html lang="en"><head><meta charset="utf-8"><title>RaftKV dashboard</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
              body{font:14px/1.4 system-ui,-apple-system,Segoe UI,sans-serif;margin:24px;color:#1d1d1f;background:#fafafa}
              h1{font-size:20px;margin:0 0 4px} .sub{color:#666;margin-bottom:20px}
              table{border-collapse:collapse;margin-bottom:24px;background:#fff}
              th,td{border:1px solid #ddd;padding:6px 10px;text-align:left;vertical-align:top}
              th{background:#f0f0f0;font-weight:600}
              .LEADER{background:#d7f5dd;font-weight:600} .FOLLOWER{background:#fff}
              .CANDIDATE,.PRE_CANDIDATE{background:#fff3c4} .LEARNER{background:#e8eefc} .down{background:#f6d5d5;color:#900}
              .small{color:#666;font-size:12px}
              .shards{display:grid;grid-template-columns:repeat(16,28px);gap:3px}
              .shard{height:28px;border-radius:4px;font-size:11px;display:flex;align-items:center;justify-content:center;color:#fff}
              .moving{outline:2px dashed #000;opacity:.6}
            </style></head><body>
            <h1>RaftKV</h1><div class="sub">live view of every node, refreshed every second</div>
            <div id="groups"></div>
            <h2 style="font-size:16px">Shard map</h2>
            <div class="small">color = owning group, dashed = moving (pulling / frozen at the old owner)</div>
            <div id="shards" class="shards" style="margin-top:8px"></div>
            <script>
            const NODES = [/*NODES*/];
            const COLORS = ["#555","#2f6fde","#d9534f","#2e9e5b","#e08a00","#8e44ad","#16a2b8","#c2185b","#6d4c41"];
            async function poll() {
              const results = await Promise.all(NODES.map(u => fetch(u + "/status.json", {cache: "no-store"})
                  .then(r => r.json()).catch(() => null)));
              const groups = new Map();
              results.forEach((r, i) => { if (!r) return; r.groups.forEach(g => {
                if (!groups.has(g.group)) groups.set(g.group, new Map()); groups.get(g.group).set(r.node, g); }); });
              const nodeIds = results.map((r, i) => r ? r.node : "?" + (i + 1));
              let html = "<table><tr><th>group</th>" + results.map((r, i) =>
                  "<th>" + (r ? "n" + r.node : NODES[i] + " (down)") + "</th>").join("") + "</tr>";
              [...groups.keys()].sort((a, b) => a - b).forEach(gid => {
                html += "<tr><th>" + (gid == 0 ? "g0 controller" : "g" + gid) + "</th>";
                results.forEach(r => {
                  const g = r && groups.get(gid).get(r.node);
                  if (!g) { html += "<td class='down'>down</td>"; return; }
                  html += "<td class='" + g.role + "'>" + g.role + "<div class='small'>term " + g.term +
                      " · commit " + g.commit_index + " · applied " + g.last_applied +
                      "<br>voters " + g.voters.join(",") + (g.learners.length ? " · learners " + g.learners.join(",") : "") +
                      (g.config_num !== undefined ? "<br>config " + g.config_num : "") + "</div></td>";
                });
                html += "</tr>";
              });
              document.getElementById("groups").innerHTML = html + "</table>";
              const owner = {}, moving = {};
              groups.forEach((byNode, gid) => byNode.forEach(g => {
                if (g.role !== "LEADER" || !g.shards_serving) return;
                g.shards_serving.concat(g.shards_gcing).forEach(s => owner[s] = gid);
                g.shards_pulling.forEach(s => { owner[s] = gid; moving[s] = true; });
              }));
              const n = Math.max(0, ...Object.keys(owner).map(Number)) + 1;
              let cells = "";
              for (let s = 0; s < n; s++) {
                const gid = owner[s];
                cells += "<div class='shard" + (moving[s] ? " moving" : "") + "' title='shard " + s + "' style='background:" +
                    (gid === undefined ? "#ccc" : COLORS[gid % COLORS.length]) + "'>" + s + "</div>";
              }
              document.getElementById("shards").innerHTML = cells;
            }
            poll(); setInterval(poll, 1000);
            </script></body></html>
            """;
}
