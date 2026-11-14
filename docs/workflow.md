# How we work

## The bundle workflow

Each step arrives as one text file, a **bundle** (`raftkv-<step>.bundle`). It holds every new or changed file for that step plus a SHA-256 checksum per file. You apply it with the unpacker that lives in this repo:

```powershell
# From the repository root (C:\Users\ajayk\Projects\raftkv)
java tools\Unbundle.java $HOME\Downloads\raftkv-<step>.bundle . --dry-run   # preview
java tools\Unbundle.java $HOME\Downloads\raftkv-<step>.bundle .             # apply
```

- Nothing is written unless **every** checksum passes, so a broken download can never leave a half-applied step.
- If a bundle changes a file that already exists, the unpacker stops and lists the files. Re-run with `--force` once you've checked that's expected (for example, a later milestone updates `README.md`).
- Bundles are listed in `.gitignore`, so they never get committed by accident.

## The loop for every step

1. Download the bundle.
2. `--dry-run`, then apply it.
3. `.\mvnw.cmd test`, then check that everything is green.
4. Read the matching `docs/learning/` explainer.
5. Commit and push:
   ```powershell
   git add -A
   git commit -m "M1: <what this step did>"
   git push
   ```

## Commit messages

`M<milestone>: <imperative summary>`, for example `M2: add randomized election timeouts`. One commit per step keeps the history readable as a learning log.

## Replaying a failing randomized test

Every simulation test prints its seed when it fails. Re-run it exactly:

```powershell
.\mvnw.cmd test "-Dtest=ElectionTest" "-Draftkv.seed=8812734"
```

(In PowerShell, quote `-D` arguments as shown, or PowerShell may split them.)
