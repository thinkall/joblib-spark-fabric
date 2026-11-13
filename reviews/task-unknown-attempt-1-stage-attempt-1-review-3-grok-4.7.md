---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 3
timestamp: "2026-11-13T12:41:46Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "6fec0e6af0cf6951411bf1f6ce667073d087ad73ab3eeed5d4158ad67c252bea"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\code\joblib-spark\reviews\task-unknown-attempt-1-stage-attempt-1-review-3-grok-4.7.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `joblibspark/backend.py` submit/apply_async (lines 261-350), pool lifecycle (lines 239-247), and the Spark Connect <4.1 session call (line 276).
- [x] Read `.github/workflows/main.yml` token permissions (lines 8-9), unconditional scikit-learn install (line 42), legacy-joblib install (line 78), fail-open tag check (lines 96-107), and artifact upload (lines 119-122).
- [x] Read `.github/workflows/release.yml` caller permissions (lines 7-12) and publish download permissions (lines 20-26).
- [x] Read `test/test_backend.py` error and session tests and `test/test_spark.py` Parallel failure assertion. Those cover `Exception` via `AsyncResult.get`, not `BaseException` or an empty Spark collect.
- [x] Read `joblibspark/utils.py` resource-profile import fallback (lines 50-55). Pre-existing; not counted below.
- [ ] Did not execute tests or workflows. Session notes that joblib 1.0.1 and 1.6.0 error paths passed are not a re-run, and GitHub CI has not run.
- [ ] PySpark and joblib sources are not in this checkout, so Spark 4.0-4.2 helper signatures were not independently confirmed.

## Issues

### Issue 1: Release artifact handoff is unauthorized
- **Severity**: High
- **File**: `.github/workflows/main.yml`, `.github/workflows/release.yml`
- **Line(s)**: main.yml 8-9, 119-122; release.yml 7-8, 20-26
- **Description**: Both workflows set `permissions: contents: read`, which sets every unspecified `GITHUB_TOKEN` scope to none. `actions/upload-artifact@v4` needs `actions: write`, and `actions/download-artifact@v4` needs `actions: read`. The publish job replaces workflow permissions with only `id-token: write`, so it also lacks `actions: read`. A called workflow cannot raise permissions above the caller, so adding `actions: write` only in `main.yml` still fails when `release.yml` calls it.
- **Risk**: The package job fails on every push and pull request, so CI cannot go green. On release, the checked distributions are never uploaded or downloaded, and Trusted Publishing never runs. This fails closed, but it blocks the release gate.
- **Suggested Fix**: Grant `actions: write` on the caller and on the package job. On the publish job set `actions: read`, `id-token: write`, and `attestations: write` together. Skip artifact upload on fork pull requests, where `actions: write` is denied, so package validation can still pass.

### Issue 2: BaseException kills a pool worker and hangs Parallel
- **Severity**: Medium
- **File**: `joblibspark/backend.py`
- **Line(s)**: 245-246, 347-349
- **Description**: The private traceback wrapper was removed so normal exceptions reach `error_callback` and `AsyncResult.get`. The raw function is now submitted to `multiprocessing.pool.ThreadPool`. That worker only catches `Exception`. `SystemExit` and `KeyboardInterrupt` are not caught, so the result is never posted and the callback never runs. `terminate` does not replace dead workers, and `atexit` only calls `close`. New tests raise `ValueError` only.
- **Risk**: A batch that calls `sys.exit`, or any other `BaseException`, permanently loses that pool thread. joblib waits for a completion callback that never arrives, so the notebook or driver hangs instead of failing the batch. Repeated hits exhaust the pool.
- **Suggested Fix**: Wrap the submitted callable so `Exception` still propagates, but any other `BaseException` is re-raised as a chained `RuntimeError` before it leaves the worker. Add a regression test that `submit` of a `SystemExit` completes `get(timeout=...)` with an error instead of hanging.

### Issue 3: Connect 4.0 workaround mutates the process-global session
- **Severity**: Medium
- **File**: `joblibspark/backend.py`
- **Line(s)**: 273-276
- **Description**: For Spark Connect before 4.1, every pool worker calls `_set_default_and_active_session(self._spark)`. That helper sets the process-wide default session as well as the thread-local active session, and nothing restores the previous default. Pool threads are reused, and the write is repeated for every batch. The active session is what this worker needs; the global default is shared with the rest of the process.
- **Risk**: A worker can race with notebook or driver code that switches sessions and can put the captured backend session back as the process default after the caller has moved on. Later `getOrCreate` calls then attach to the wrong session. There is no rollback if the batch fails after the write.
- **Suggested Fix**: On the worker thread, set only the thread-local active session when that API exists. If the combined helper is required, save the previous default and restore it in `try/finally` after the Spark collect. Do not leave task teardown dependent on the next batch overwriting global state.

### Issue 4: Release version gate fails open
- **Severity**: Medium
- **File**: `.github/workflows/main.yml`, `.github/workflows/release.yml`
- **Line(s)**: main.yml 96-107; release.yml 11-12
- **Description**: The tag check reads `github.event.release.tag_name` inside the reusable workflow and ignores an empty tag (`if tag and tag != ...`). `release.yml` does not pass the tag as a `workflow_call` input. An empty expression is indistinguishable from a normal push or pull request, where skipping the check is intentional. Reusable-workflow `github.event` is not a reliable source for the caller release payload.
- **Risk**: A published tag that does not match `joblibspark.__version__` can pass the gate and be uploaded. PyPI then serves the package version from metadata, not the GitHub tag. A mismatch is not corrected by a later tag, and a repeated version fails only after approval.
- **Suggested Fix**: Pass `release_tag` explicitly from `release.yml`. On that path, fail if the input is empty or is not `v` plus `joblibspark.__version__`. Keep the empty-tag skip only for non-release events.

### Issue 5: CI can block or skip supported failure-path combinations
- **Severity**: Low
- **File**: `.github/workflows/main.yml`
- **Line(s)**: 42, 58-65, 78
- **Description**: The Spark matrix always installs scikit-learn, including Python 3.14, while `test_spark.py` treats it as optional via `importorskip`. The legacy matrix installs `.[connect]`, which resolves PySpark 4.x only, so joblib 1.0.1 is never run against classic Spark 3.5 in CI. That pair uses the callable-only inheritance path plus the wrapper removal, and it was only reported as a local run.
- **Risk**: A missing or conflicting scikit-learn build fails every Python 3.14 Spark cell before backend tests. A regression limited to Spark 3.5 and joblib 1.0.1 is not gated by the workflow that must be green before release.
- **Suggested Fix**: Install scikit-learn in a non-fatal step, matching `importorskip`. Add one legacy cell that pins PySpark 3.5 and joblib 1.0.1 on Python 3.11 with Java 17, classic mode only.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver resolutions

These dispositions supersede the initial Open placeholders. See lens 1 for the
single retained effort decision, authoritative artifact API checks, and validation.

| Issue | Disposition | What, why, and verification |
| --- | --- | --- |
| 1 | Rejected: incorrect API premise | Same-run upload/download uses the Actions runtime token. No cross-run GitHub-token request or GitHub attestation-store API is used; extra Actions/attestations write privileges are not required. |
| 2 | Fixed | Reproduced lost completion after a driver SystemExit. Added a narrow thread-boundary guard that leaves ordinary exceptions unchanged and converts other BaseException instances to chained RuntimeError. The result and error callback are both asserted, with no timeout or unhandled worker exit after the fix. |
| 3 | Rejected: source contradicts the overwrite claim | Spark 4.0's helper takes a lock and assigns the global default only when it is None; the thread-local active session is likewise initialized only when absent. It does not overwrite a different existing default on every batch. This preserves the upstream fork's prior 4.0 workaround. Verified against `apache/spark` tag `v4.0.0`, `python/pyspark/sql/connect/session.py`, `_set_default_and_active_session`. |
| 4 | Rejected: documented caller context | GitHub explicitly documents that a reusable workflow's github context belongs to the caller, including its release event. A published release has a tag; ordinary push/PR checks intentionally allow an empty release tag. The version check rejects a present mismatch. See the official reference linked in lens 1. |
| 5 | Fixed coverage gap; rejected speculative dependency failure | Added the legacy 3.5 cell. Current scikit-learn 1.9.1 declares Python 3.14 and provides 12 cp314 wheels on PyPI, so silently making the main matrix's requested sklearn validation optional would hide regressions. Legacy jobs still intentionally skip sklearn when absent. |
