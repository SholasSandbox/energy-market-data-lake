# Browser Use Node REPL Setup

Purpose: capture the steps needed to expose `mcp__node_repl__js` for Codex in-app browser control, without interrupting the ENTSOG gas build flow.

## Current Status

The Browser Use plugin is enabled locally:

```toml
[plugins."browser-use@openai-bundled"]
enabled = true
```

Confirmed in:

```text
~/.codex/config.toml
```

The plugin package is present, including:

```text
~/.codex/plugins/cache/openai-bundled/browser-use/0.1.0-alpha1/.codex-plugin/plugin.json
~/.codex/plugins/cache/openai-bundled/browser-use/0.1.0-alpha1/scripts/browser-client.mjs
~/.codex/plugins/cache/openai-bundled/browser-use/0.1.0-alpha1/skills/browser/SKILL.md
```

The missing capability in the active Codex session is the Node REPL JavaScript execution tool, usually exposed to Codex as:

```text
mcp__node_repl__js
```

Because that tool is not surfaced in the active session, Codex cannot bootstrap the Browser Use in-app browser runtime from this chat.

Tool discovery was attempted for:

```text
node_repl js JavaScript execution mcp__node_repl__js browser-use
```

It surfaced connector tools only, not the Node REPL `js` tool.

Conclusion: the Browser Use plugin install is present, but the current Codex session was started without the required Node REPL execution tool. This is a session/tool-exposure issue, not a repo code issue.

## Steps To Try Later

1. Confirm Browser Use remains enabled:

   ```bash
   rg -n 'browser-use' ~/.codex/config.toml
   ```

2. Reload the VS Code extension host so Codex gets a fresh tool list.

   Recommended route:

   ```text
   Command Palette -> Developer: Reload Window
   ```

3. Start a fresh Codex chat after the reload. Tool availability is decided at session start, so the existing chat is unlikely to gain `mcp__node_repl__js` mid-session.

4. Explicitly trigger Browser Use in the new chat:

   ```text
   @browser-use open http://127.0.0.1:5173
   ```

   Alternative wording:

   ```text
   Use Browser Use to open http://127.0.0.1:5173
   ```

5. Ask Codex to verify whether the Node REPL tool is visible:

   ```text
   Check whether mcp__node_repl__js is exposed in this session.
   ```

6. If it is visible, the first Browser Use cell should import the plugin's browser client from the absolute plugin cache path and initialize the `iab` backend:

   ```js
   const { setupAtlasRuntime } = await import("/Users/shola/.codex/plugins/cache/openai-bundled/browser-use/0.1.0-alpha1/scripts/browser-client.mjs");
   const backend = "iab";
   await setupAtlasRuntime({ globals: globalThis, backend });
   ```

## Expected Result

Codex should be able to use Browser Use through the Node REPL JavaScript execution tool and control the in-app browser directly.

The internal setup uses the Browser Use plugin's `browser-client.mjs` with the `iab` backend.

## If It Still Does Not Appear

Try these in order:

1. Update the OpenAI / Codex VS Code extension.
2. Restart VS Code completely.
3. Start a new Codex chat.
4. Trigger Browser Use again with `@browser-use`.

Avoid manually adding a fake `node_repl` MCP server unless there is official extension documentation for the exact server command. The Browser Use plugin currently provides the browser client and skill files, but not an obvious standalone Node REPL server command to wire safely by hand.

If the tool still does not appear after a full extension restart and a new chat, treat this as an extension/plugin runtime issue and continue using the fallback below for local dashboard QA.

## Acceptable Fallback

For this project, local browser QA can still be completed with Chrome headless:

```bash
mkdir -p docs/evidence/screenshots /tmp/chrome-phase7-gas
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless=new \
  --disable-gpu \
  --hide-scrollbars \
  --user-data-dir=/tmp/chrome-phase7-gas \
  --window-size=1920,1800 \
  --screenshot=docs/evidence/screenshots/dashboard-phase7-gas-context-20260507.png \
  http://127.0.0.1:5173/
```

This fallback is suitable for screenshot evidence and layout checks, but it does not provide interactive in-app browser control.
