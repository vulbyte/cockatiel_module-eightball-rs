# cockatiel_module-eightball-rs

A tiny Cockatiel module: the Magic 8-Ball.

## Command
- `!8ball <question>` — replies with a classic random answer (20 possible).

Reply example: `🎱 bob: It is certain.`

## Config (`config.json` → `module_specific`)
- `command_flag` — the command prefix, default `!`
- `reconnect_base_secs` / `reconnect_max_secs` — reconnect backoff bounds

## Build
```sh
cargo build --release
```
Then start it from the TUI (Modules window → select `eightball` → `s`), or it autostarts.
