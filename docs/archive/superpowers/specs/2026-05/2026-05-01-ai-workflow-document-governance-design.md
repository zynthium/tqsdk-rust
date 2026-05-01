# AI Workflow Document Governance Design

> Archived on 2026-05-01.
> Current architecture authority lives in `docs/architecture/*`.

**Date:** 2026-05-01
**Status:** Approved design draft
**Scope:** AI workflow commit and archival discipline for code-driving documentation

## 1. Summary

This design tightens the AI workflow so future coding agents leave work in a committed state and do not keep completed execution docs in active directories.

It adds two default rules:

- when a code-change batch reaches a coherent, verified checkpoint, create a commit before moving on
- when a spec/plan/review doc has finished driving its code change, archive it to `docs/archive/superpowers/`

## 2. Goals

- Make completed code changes easy to recover and review.
- Prevent execution docs from lingering in active locations after they have served their purpose.
- Keep current architecture authority active and untouched.
- Align with the superpowers spec-driven / plan-driven workflow: design/spec -> plan -> implementation -> verification -> commit -> archive.

## 3. Non-Goals

- Do not auto-archive `docs/architecture/*`.
- Do not auto-archive live reviews or scenario docs that still govern open work.
- Do not change runtime semantics, crate boundaries, or public API.
- Do not turn every tiny edit into a commit; commit at coherent checkpoints.

## 4. Chosen Approach

- Update `docs/architecture/ai-workflow.md` with explicit commit and archive discipline.
- Mirror the rule in repository entry docs (`docs/README.md`, `docs/superpowers/README.md`, archive README, `CLAUDE.md`, `AGENTS.md`).
- Treat superpowers plans/specs as execution records that are archived after closure, not as permanent live backlog.

## 5. Success Criteria

- Future AI sessions can tell when to commit and when to archive.
- Current authority docs stay active.
- Historical execution docs migrate cleanly to `docs/archive/superpowers/`.
