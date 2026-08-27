@AGENTS.md

# Claude Code

The line above imports [AGENTS.md](AGENTS.md), the shared instruction file every
coding agent in this repository follows — project layout, the navigation table,
architecture invariants, the rules and verification. Claude Code does not read
`AGENTS.md` on its own; that import is what puts it in context.

**Do not copy shared content into this file.** Anything both Codex and Claude
need goes in `AGENTS.md`; this file holds only what is specific to Claude Code.
Two files describing the same repository is how they start contradicting each
other.

## Before handing work back

- `/code-review` on the diff before the branch is proposed for a PR into `dev`.
- `/security-review` when the change touches broker delivery, credentials,
  `.env` handling, or anything published to NATS.
- Update `changelog.md` and the affected `README.md` section in the same change,
  per AGENTS.md rule 4.

## Where to slow down

Use plan mode, and confirm the approach, before editing:

- `worker/gateways/{forex,crypto}/executor.py` — the order path; a mistake here
  places, mis-sizes or fails to close a real position.
- `worker/gateways/signal_handler.py` and `worker/gateways/guard.py` — which
  flow an action takes and which entries are refused.
- `worker/db/schema.py` — the tables carry live positions; a change must upgrade
  an existing database in place, never recreate it.
- `worker/settings.py` — a settings change reaches every deployment through
  `.env`, and a changed default silently changes how running workers trade.

## Session hygiene

- Durable project facts belong in `AGENTS.md`, not in auto memory: Codex has to
  see them too.
- If `/context` does not list both `CLAUDE.md` and `AGENTS.md` under **Memory
  files**, the import broke — check that the first line of this file is
  `@AGENTS.md` outside any code fence.
