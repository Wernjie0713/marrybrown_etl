# AGENT.md

## Agent basics

- Prioritise PowerShell (`pwsh.exe`).
- MUST NOT install dependencies automatically; ask the user first.
- MUST NOT use Unix tools (e.g., `sed`); use native PowerShell commands instead.
- Search file names with `fd`; search content with `rg`; search code structure with `sg`.
- Keep existing changes intact; do not revert unrelated user edits.
