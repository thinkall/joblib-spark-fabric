---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 4
timestamp: "2026-11-13T12:56:43Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "763a85ed7905bb6a9d0510bd403ed0ef09921d8f6cd7cd556cda368b5a257654"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\code\joblib-spark\reviews\task-unknown-attempt-1-stage-attempt-1-review-4-grok-4.7.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared `joblibspark/backend.py` lines 264-351 with PySpark 3.5.7 and 4.3.0 `pyspark/util.py` `inheritable_thread_target`, and with joblib 1.0.1 and 1.6.0 result retrieval through `AsyncResult.get()`. The new `submit` delegation, Connect `< 4.1` session call, and `error_callback=callback` path match those contracts.
- [x] Confirmed joblib 1.6.0 `.venv/Lib/site-packages/joblib/parallel.py` stores `_backend_kwargs` and calls `abort_everything(ensure_ready=self._managed_backend)`. `joblibspark/backend.py` line 228 still reads `_backend_args`, which exists in joblib 1.0.1 only.
- [x] Read `.github/workflows/release.yml` lines 14-28 and `.github/workflows/main.yml` lines 6-8 and 119-121. Distributions are uploaded inside the called workflow and downloaded by a job whose only permission is `id-token: write`.
- [x] Counted the new title underlines in `CHANGES.rst` lines 4-5 and `doc/installing.rst` lines 1-2. Both are one character shorter than their titles.
- [x] Read `joblibspark/backend.py` lines 122-155 and `test/test_backend.py` lines 140-221. The regression tests mock `inheritable_thread_target`, so they do not prove the Spark helper signature.
- [ ] Spark 4.0-4.2 helper sources are not installed locally. The session-first classic path was checked only against PySpark 4.3.0.
- [ ] The release workflow was not executed. The artifact-download failure is inferred from the workflow and the `download-artifact` v4 reusable-workflow contract.
- [ ] Linux GPU tests and the full GitHub matrix were not run in this read-only review.

## Issues

### Issue 1: Publish job is not configured to download the called-workflow artifact
- **Severity**: High
- **File**: .github/workflows/release.yml
- **Line(s)**: 20-26
- **Description**: New release path. `publish` replaces permissions with only `id-token: write`, so `actions` is none. `python-package-distributions` is uploaded by the `package` job inside the reusable workflow `.github/workflows/main.yml` (line 119), not by a job in `release.yml`. The download step passes neither `run-id` nor `github-token`. `actions/download-artifact@v4` requires those inputs and `actions: read` for an artifact produced by a reusable workflow.
- **Risk**: CI can succeed and the PyPI publish step still fail to find the built distributions, so the release workflow does not publish v0.7.0.
- **Suggested Fix**: Give `publish` `actions: read` and `id-token: write`, and pass `run-id: ${{ github.run_id }}` and `github-token: ${{ github.token }}` to `actions/download-artifact@v4`.

### Issue 2: abort_everything still reads removed joblib `_backend_args`
- **Severity**: Medium
- **File**: joblibspark/backend.py
- **Line(s)**: 224-228
- **Description**: Pre-existing, not introduced by this diff, but it breaks a supported current-joblib path. When `ensure_ready` is true, reconfiguration unpacks `self.parallel._backend_args`. joblib 1.0.1 has that attribute. joblib 1.6.0 renamed it to `_backend_kwargs` and passes `ensure_ready=self._managed_backend`, which is true for `with Parallel(...)`. `parallel_backend()` leaves the backend unmanaged, so the existing error tests do not hit this branch.
- **Risk**: A batch failure inside a managed `Parallel` context on joblib 1.6 replaces the original exception with `AttributeError` while aborting.
- **Suggested Fix**: Read `_backend_kwargs` when present and fall back to `_backend_args` for joblib 1.0.1, then pass that dict to `configure`.

### Issue 3: New RST title underlines are one character short
- **Severity**: Low
- **File**: CHANGES.rst
- **Line(s)**: 4-5
- **Description**: New text. `Release 0.7.0 (joblibspark-fabric)` is 34 characters and its underline is 33. `doc/installing.rst` lines 1-2 have the same off-by-one: `Installing joblibspark-fabric` is 29 characters and the underline is 28. Docutils requires the underline to be at least as long as the title. Longer underlines elsewhere in these files are valid.
- **Risk**: Sphinx or `rst2html` treats the new section titles as malformed and can fail the documentation build.
- **Suggested Fix**: Add one `-` to `CHANGES.rst` line 5 and one `=` to `doc/installing.rst` line 2.

### Issue 4: Explicit zero CPU or GPU counts are treated as unset
- **Severity**: Low
- **File**: joblibspark/backend.py
- **Line(s)**: 154-155
- **Description**: Pre-existing. `num_cpus_per_spark_task or default_cpus_per_task` and the same expression for GPUs treat `0` as missing. `None` is the unset value. `0 or default` therefore substitutes the cluster default.
- **Risk**: An explicit request for 0 CPUs, or 0 GPUs when the cluster default is non-zero, silently builds the wrong resource profile.
- **Suggested Fix**: Replace each `or` with an `is None` check before applying the default.

### Issue 5: Non-local classic Spark always warns that stage scheduling is unsupported
- **Severity**: Low
- **File**: joblibspark/backend.py
- **Line(s)**: 131-135
- **Description**: Pre-existing. In the non-local branch, `support_stage_scheduling` is the `withResources` check, but the warning is emitted unconditionally before that flag is returned. A cluster that has `withResources` still warns that the Spark version does not support stage-level scheduling.
- **Risk**: Supported classic deployments emit a false warning, and callers can disable or ignore stage scheduling based on that message.
- **Suggested Fix**: Warn only when `support_stage_scheduling` is false.

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

These dispositions supersede the initial Open placeholders. The complete effort
decision, stage outcomes, and verification are retained in lens 1.

| Issue | Disposition | What, why, and verification |
| --- | --- | --- |
| 1 | Rejected: same-run artifact | Reusable workflow jobs share the caller run identity; this is not another workflow run. Download-artifact v4 defaults to current-run runtime-token access. Explicit run-id/GITHUB_TOKEN/Actions scopes are only needed for cross-run/repository access. Official sources are linked in lens 1. |
| 2 | Fixed | Reproduced the current-joblib managed abort failure, preserved old/new kwargs names, and reset the aborted pool to prevent stale callbacks corrupting a subsequent call. Unit and real managed recovery tests pass for joblib 1.0.1 and 1.6. |
| 3 | Fixed | New heading underlines now match their title lengths exactly, verified as 34/34 and 29/29. |
| 4 | Deferred: unchanged baseline behavior | Independently verified at baseline commit `6e9931f7`, backend.py line 165 and the adjacent GPU assignment. Zero-resource semantics are outside this version-compatibility and release change. |
| 5 | Deferred: unchanged baseline diagnostic | Independently verified at baseline commit `6e9931f7`, backend.py line 146. No introduced scheduling behavior depends on changing this warning. |
