---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 2
timestamp: "2026-11-13T12:44:56Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "b063892dc9fcec14ed2030c33473334fc4623d39b6c35d585bbb6e55b311afee"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\code\joblib-spark\reviews\task-unknown-attempt-1-stage-attempt-1-review-2-grok-4.7.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `joblibspark/backend.py` submission path (lines 100-112, 224-228, 261-276, 338-349) and compared it with joblib 1.6.0 `ParallelBackendBase`, `PoolManagerMixin.submit`, `BatchCompletionCallBack`, and `Parallel._abort` in `.venv\Lib\site-packages\joblib`.
- [x] Read PySpark 4.3.0 `inheritable_thread_target` in `.venv\Lib\site-packages\pyspark\util.py` and checked the session-decorator versus callable-only contract against the new branch.
- [x] Read `.github/workflows/main.yml` permissions and `upload-artifact` step, and `.github/workflows/release.yml` caller permissions plus the publish job's `download-artifact` step.
- [x] Read `joblibspark/__init__.py` import wrapper, `setup.py` extras, `requirements.txt`, `README.md` support table, and the `legacy-joblib` install in `main.yml`.
- [ ] Did not execute tests or CI. Session validation was supplied only. Did not read sibling review artifacts.

## Issues

### Issue 1: Release artifact handoff is not permitted to succeed
- **Severity**: High
- **File**: `.github/workflows/main.yml`, `.github/workflows/release.yml`
- **Line(s)**: main.yml 8-9, 119-122; release.yml 7-8, 20-26
- **Description**: Newly introduced. The publish design builds once inside the reusable CI workflow, uploads `python-package-distributions`, and publishes that artifact. Both workflows set an explicit `permissions` block, which sets every omitted GITHUB_TOKEN scope to none. `main.yml` grants only `contents: read`, but `actions/upload-artifact@v4` requires `actions: write`. The publish job then replaces permissions with only `id-token: write`, while `actions/download-artifact@v4` requires `actions: read`. Called workflows also cannot exceed the caller's grants, and `release.yml` itself only grants `contents: read`.
- **Risk**: The Trusted Publishing path fails closed: the validated distributions are never uploaded, or cannot be downloaded, so v0.7.0 cannot be published as designed. This is not a wrong-package publish.
- **Suggested Fix**: Grant `actions: write` on the package job and on the release caller that invokes it. Grant `actions: read` on the publish job next to `id-token: write`. Keep the build-once artifact handoff. Do not rebuild inside the publish job.

### Issue 2: Abort reconfigure still uses joblib's removed private attribute
- **Severity**: Medium
- **File**: `joblibspark/backend.py`
- **Line(s)**: 224-228
- **Description**: Pre-existing, and not updated by the new `submit` adapter. `abort_everything(ensure_ready=True)` splats `self.parallel._backend_args`. Joblib 1.6.0 stores those kwargs on `_backend_kwargs` and has no `_backend_args`. `Parallel._abort` passes `ensure_ready=self._managed_backend`, which is true only when `Parallel` is used as a context manager. The usual `Parallel()(iterable)` path skips this line, which is why the supplied error tests can pass.
- **Risk**: On current joblib, a task failure inside `with Parallel(...)` raises `AttributeError` from `_abort()` and masks the original batch exception. The backend is also left unreconfigured for a reused managed instance.
- **Suggested Fix**: Read `_backend_kwargs` and fall back to `_backend_args` for joblib 1.0-1.2. Add a regression that uses `with Parallel(n_jobs=2) as parallel` and expects the original task exception on both an old joblib and joblib 1.6.

### Issue 3: Registration errors always prescribe the spark extra
- **Severity**: Medium
- **File**: `joblibspark/__init__.py`
- **Line(s)**: 30-36
- **Description**: The catch-all is pre-existing. The new message is not. `register_spark` turns every `ImportError` from importing `backend` into advice to install `joblibspark-fabric[spark]`. That import also loads `joblib`, `packaging`, and `py4j`, which are not what the spark extra uniquely provides. `raise ... from exc` keeps the chain, but the outer message is what callers see. Connect-only failures are also pointed at `[spark]`, not `[connect]`.
- **Risk**: A missing base dependency or an internal import break is misdiagnosed as "install PySpark". Managed Fabric users may install or replace a runtime they were told not to replace.
- **Suggested Fix**: Rewrite the error only when the missing module is `pyspark` or `py4j`. Re-raise other import failures unchanged. Mention `[connect]` only for a connect extra module.

### Issue 4: New thread-context choice does not follow the capability-flag pattern
- **Severity**: Low
- **File**: `joblibspark/backend.py`
- **Line(s)**: 100-112, 273-276, 338-345
- **Description**: New code, behavior matches the intended Spark 4.3 contract. Existing mode decisions are instance flags set in `__init__`. The new helper choice and the Spark 4.0 active-session shim re-parse `pyspark.__version__` on every submission and inside every pool task. `_spark_pinned_threads_enabled` is set only in the classic branch, then read by the new condition. Connect mode works only because `or` short-circuits.
- **Risk**: A later reorder of that boolean raises `AttributeError` in Connect. The compatibility rule stays implicit instead of sitting with the other backend capabilities.
- **Suggested Fix**: In `__init__`, always set `_spark_pinned_threads_enabled`, plus `_inherit_thread_with_session` and `_propagate_connect_active_session`. Use those flags in `apply_async`. Keep the callable-only path for classic 3.5 and pinned threads off.

### Issue 5: CI does not cross the special Spark branches with legacy joblib
- **Severity**: Low
- **File**: `.github/workflows/main.yml`
- **Line(s)**: 59-78
- **Description**: Coverage gap, not a code regression. `legacy-joblib` installs `.[test,connect]`, which resolves to PySpark `>=4.0,<=4.3.0` on Java 21, then runs both modes. It never runs Spark 3.5's callable-only helper, and it never runs the Spark 4.0 `< (4, 1)` active-session shim. The main matrix covers those Spark lines only with unpinned latest joblib. Supplied local evidence covers 3.11/joblib 1.0.1/Spark 3.5.7, but that cell is not in the release gate. Python 3.14 with joblib 1.5.3 was not verified here as a supported pair.
- **Risk**: A green CI run does not lock the two compatibility branches this release adds against the old joblib callback contract.
- **Suggested Fix**: Add a small legacy cell for Python 3.11, joblib 1.0.1, Spark 3.5 classic, Java 17, and one Spark 4.0 Connect cell. Do not treat the current legacy job as coverage of those branches.

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

These dispositions supersede the initial Open placeholders without changing the
original findings or verdict. The effort decision and aggregate gate are retained
in the lens-1 artifact.

| Issue | Disposition | What, why, and verification |
| --- | --- | --- |
| 1 | Rejected: incorrect API premise | Same-run artifact v4 transfer uses `ACTIONS_RUNTIME_TOKEN`; it does not require the proposed GITHUB_TOKEN write scopes. Official download-artifact documentation and artifact client source were checked; see lens 1. |
| 2 | Fixed | Reproduced the AttributeError on actual joblib 1.6. Added `_backend_kwargs`/`_backend_args` compatibility and aborted-pool cleanup. Unit cases cover both names. Real old/current managed Parallel error and reuse tests pass. |
| 3 | Fixed | Only missing root PySpark/py4j modules get standalone-extra advice, with an explicit managed-runtime warning. Other import failures propagate unchanged. Both cases are tested. |
| 4 | Deferred: non-blocking design preference | Short-circuiting is intentional and tested across Connect, pinned classic Spark 4, classic Spark 3.5, and unpinned mode. Parsing a local version string adds no network operation. Additional cached capability state is unnecessary for this bounded fix. |
| 5 | Fixed | Added explicit legacy Spark 3.5/classic/Java 17 and Spark 4.0/Connect cells. Existing current-Python cells remain, and full CI is a release gate rather than claimed completed evidence. |
