# Documentation Archive And AI Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize repository documentation so current architecture is authoritative, review records are discoverable but not mistaken for authority, and future AI code assistants follow the correct guardrails.

**Architecture:** Keep `docs/architecture/*` as the canonical design source. Move review artifacts into `docs/reviews/` or `docs/archive/`, add explicit index documents, and update AI workflow entry points so agents distinguish architecture authority from historical review input.

**Tech Stack:** Markdown documentation, Cargo workspace metadata, git.

---

### Task 1: Create Documentation Taxonomy

**Files:**
- Create: `docs/README.md`
- Modify: `README.md`
- Modify: `docs/architecture/README.md`

- [x] **Step 1: Add a docs root index**

Create `docs/README.md` with sections for:

- `architecture/`: current architecture authority.
- `scenarios/`: scenario contracts and API gaps.
- `reviews/`: current review and decision records.
- `archive/`: historical review inputs.
- `superpowers/`: specs and plans as execution artifacts.

- [x] **Step 2: Update repository README**

Update the root `README.md` documentation section so users and agents enter through `docs/README.md` before drilling into architecture, scenarios, reviews, or plans.

- [x] **Step 3: Update architecture README**

Update `docs/architecture/README.md` to state that architecture docs override review reports when there is conflict, and that review records are decision inputs unless promoted into architecture docs.

### Task 2: Move Review Artifacts To Stable Locations

**Files:**
- Move: `docs/public-api-scenario-review.md` -> `docs/reviews/public-api-scenario-review.md`
- Move: `docs/public-api-disposition-matrix.md` -> `docs/reviews/public-api-disposition-matrix.md`
- Move: `docs/public-api-overdesign-audit.md` -> `docs/archive/reviews/2026-04-29/public-api-overdesign-audit.md`
- Move: `docs/review-2026-04-29-pending.md` -> `docs/archive/reviews/2026-04-29/review-2026-04-29-pending.md`
- Create: `docs/reviews/README.md`

- [x] **Step 1: Move active review records**

Move scenario review and disposition matrix into `docs/reviews/`.

- [x] **Step 2: Archive raw review inputs**

Move the original 2026-04-29 audit inputs into `docs/archive/reviews/2026-04-29/`.

- [x] **Step 3: Add review index**

Create `docs/reviews/README.md` explaining which documents are active decision records and which archived reports are historical inputs.

### Task 3: Synchronize AI Workflow And Scenario References

**Files:**
- Modify: `docs/architecture/ai-workflow.md`
- Modify: `docs/scenarios/README.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`
- Modify: `docs/architecture/validation.md`
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Create: `docs/superpowers/README.md`
- Modify: `docs/superpowers/plans/*.md` when old paths must remain machine-discoverable.

- [x] **Step 1: Update AI workflow guardrails**

Make `docs/architecture/ai-workflow.md`, `AGENTS.md`, and `CLAUDE.md` explicitly say that archived review reports are inputs, not authority.

- [x] **Step 2: Update active scenario links**

Update scenario and validation docs to link to `docs/reviews/public-api-scenario-review.md`.

- [x] **Step 3: Add superpowers index**

Create `docs/superpowers/README.md` describing how agents should use `specs/` and `plans/` without overriding architecture docs.

- [x] **Step 4: Update stale path references**

Run `rg` for old root-level review paths and update references where the reference is intended to be live.

### Task 4: Verify And Commit

**Files:**
- Verify all changed markdown files.

- [x] **Step 1: Check path references**

Run:

```bash
rg -n "docs/public-api-|docs/review-2026|\\.\\./public-api-|public-api-scenario-review\\.md|public-api-disposition-matrix\\.md|public-api-overdesign-audit\\.md|review-2026-04-29-pending\\.md" docs README.md ROADMAP.md AGENTS.md CLAUDE.md
```

Expected: any remaining old-path references are either historical command snippets in archived plans or have been intentionally updated.

- [x] **Step 2: Check markdown diff formatting**

Run:

```bash
git diff --check
```

Expected: no whitespace errors.

- [x] **Step 3: Review status and commit**

Run:

```bash
git status --short
git add README.md AGENTS.md CLAUDE.md docs
git commit -m "docs: organize architecture and review records"
```

Expected: one documentation-only commit.
