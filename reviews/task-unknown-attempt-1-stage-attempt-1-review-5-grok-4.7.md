---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 5
timestamp: "2026-11-13T12:28:26Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "68aa8a994b73b677006b56352f517d5dc69748aa84c44fed20c1422cc55fc547"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\code\joblib-spark\reviews\task-unknown-attempt-1-stage-attempt-1-review-5-grok-4.7.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared `joblibspark/backend.py` lines 261-351 (`submit`, Connect `<4.1` session set, `inheritable_thread_target` branch, `error_callback`) with `test/test_backend.py` lines 122-222
- [x] Checked mock call chains and assertions in `test_submission_inherits_spark_session`, `test_connect_submission_propagates_active_session`, `test_submission_propagates_errors`, and `test_submission_without_pinned_threads`
- [x] Read integration locks in `test/test_spark.py` lines 81-141 and the CI matrix in `.github/workflows/main.yml` lines 17-29 and 53-78
- [x] Distinguished supplied execution evidence (Spark 3.5 classic; Spark 4.3 classic and Connect) from the unrun Spark 4.0 Connect cell and from unsupported products (Spark 3.5 Connect, Python 3.14 with Spark 4.0)
- [ ] Did not execute pytest; pending GitHub CI is not treated as runtime evidence

## Issues

### Issue 1: Connect 4.0 session test does not lock the worker-thread failure mode
- **Severity**: High
- **File**: test/test_backend.py
- **Line(s)**: 165-186
- **Description**: `test_connect_submission_propagates_active_session` is the only automated lock for the new Spark Connect `<4.1` call to `_set_default_and_active_session` (`joblibspark/backend.py` lines 273-276). It patches `inheritable_thread_target` to identity, so it never composes that setter with `inheritable_thread_target(session)` (lines 338-342). `mapInPandas` is stubbed to return a pre-pickled 42, so `lambda: 42` and `mapper_fn` never run. The assertion only checks that the private method was called once with the session. It does not record the calling thread, and it does not require the setter to precede `range()`. The expected branch also recopies `release < (4, 1)` and only samples 4.0.0 and 4.3.0, so 4.1.0 and other 4.0.x builds are unasserted. Supplied validation executed Spark 4.3 Connect, which skips this branch. The `spark 4.0.*` / `connect: true` CI cell has not run. The legacy job installs `.[connect]`, which resolves a 4.x release up to 4.3.0 and therefore also skips the setter.
- **Risk**: A regression that sets the session on the caller thread, or after `range()`, stays green while Spark Connect 4.0 still has no active session on the worker. That is a supported path with no executed runtime evidence.
- **Suggested Fix**: Drive the worker with a fake `inheritable_thread_target(session)` that records the child thread and then calls the function. Make `mapInPandas` invoke the mapper. Assert the setter runs on that child thread before `range()`, for 4.0.0 and another 4.0.x version, and not for 4.1.0 or 4.3.0. Do not treat the unrun 4.0 Connect CI cell as current evidence.

### Issue 2: Inheritance tests never check which callable is submitted
- **Severity**: Medium
- **File**: test/test_backend.py
- **Line(s)**: 122-150, 205-222
- **Description**: `test_submission_inherits_spark_session` stubs the pool and only inspects how `inheritable_thread_target` was called. It never asserts `pool.apply_async` received the wrapper, or that both `callback` and `error_callback` were forwarded (`joblibspark/backend.py` lines 348-351). The expected branch recopies the production condition, so it cannot catch a wrong condition that the test also copies. On the callable-only branch, `assert callable(...)` is true for a `MagicMock` session as well as for the worker, and the test does not require `inherit.return_value` to have been applied. `test_submission_without_pinned_threads` executes only the non-default `_spark_supports_job_cancelling=False` path. Its `SparkContext._gateway` and `is_remote` patches are unused by `apply_async`. Because both the pinned-thread path and the no-cancel path end in `.collect()[0]`, the test cannot tell them apart, and the pre-canned collect value means `mapper_fn` never runs. There is no executing unit test for classic pinned threads, which is the Spark 4 path that must use the session form of the helper.
- **Risk**: Computing a wrapper and submitting the raw function, or taking the pinned-thread `setLocalProperty` path by mistake, still passes. Spark 4.3 classic integration covers the default pinned path only where that suite was actually run, not this unit lock.
- **Suggested Fix**: Parametrize an explicit expected call style. Assert `apply_async` was called with the wrapper and both callbacks. On the callable-only branch, assert the argument is the worker and is not `_spark`, and that the returned wrapper was applied. Add an executing pinned-thread case that requires `setLocalProperty("spark.jobGroup.id", ...)` inside the wrapped call and rejects `collectWithJobGroup`.

### Issue 3: Error unit test does not lock the joblib callback contract
- **Severity**: Medium
- **File**: test/test_backend.py
- **Line(s)**: 189-202
- **Description**: `test_submission_propagates_errors` checks only `AsyncResult.get()` after a Connect `range()` failure, with no callback installed. The new pool call passes the same object as `callback` and `error_callback` (`joblibspark/backend.py` lines 348-351). Dropping `error_callback` still makes `.get()` raise, so this test stays green. It also does not cover a classic-path failure, an exception raised by the user callable inside `mapper_fn`, or `_is_running is False`. This is not an unverified regression on the executed environments: `test/test_spark.py` lines 94-96 now match `condition evaluated to True` inside the spark backend, and the supplied runs passed that path on joblib 1.0.1 / Spark 3.5 classic and joblib 1.6 / Spark 4.3 classic and Connect.
- **Risk**: Callback-only joblib retrieval can hang or miss the batch failure while the unit test still passes. The integration match does not run for every legacy joblib version, and it is not a unit lock on `error_callback`.
- **Suggested Fix**: Pass a recording callback. Assert success invokes it once with the unpickled result, and failure invokes it once with the `ValueError` while `.get()` raises. Add one classic mapper failure and one `_is_running is False` case. Keep the integration message match; do not treat it as covering the callback argument.

### Issue 4: CI omits the legacy-joblib and Spark 3.5 cross-product
- **Severity**: Medium
- **File**: .github/workflows/main.yml
- **Line(s)**: 17-29, 53-78
- **Description**: `legacy-joblib` always installs `.[test,connect]`, so pip selects some PySpark `>=4.0,<=4.3.0`, then runs both connect modes on Java 21. It never installs PySpark 3.5. The callable-only helper path plus callback-style `apply_async` used by joblib 1.0.1-1.3.2 is therefore not a CI gate. The main matrix tests Spark 3.5 only with whatever latest `joblib>1.0` pip resolves. Spark 4.0, 4.1, and 4.2 run only on Python 3.11. Spark 3.5 Connect and Python 3.14 with Spark 4.0 are unsupported and correctly absent. joblib 1.0.1 with Spark 3.5 was executed locally, but that cross-product is not what green CI will prove.
- **Risk**: A green CI run can still miss a Spark 3.5 regression that only appears with legacy joblib's callback contract, and the 4.0 session branch remains a single unrun Python 3.11 cell.
- **Suggested Fix**: Add a Python 3.11 / joblib 1.0.1 / `pyspark==3.5.*` / Java 17 job with `TEST_SPARK_CONNECT=false`. Pin at least one Connect legacy run to `pyspark==4.0.*` so the `<4.1` setter is not skipped by resolving 4.3.0. Do not add the unsupported Spark 3.5 Connect or Python 3.14 / Spark 4.0 products.

### Issue 5: New register_spark ImportError path has no test
- **Severity**: Low
- **File**: joblibspark/__init__.py
- **Line(s)**: 33-37
- **Description**: The rewritten `ImportError` message and `raise ... from exc` chaining are new and untested. No test forces `from .backend import register` to fail and checks the message or `__cause__`.
- **Risk**: A broken import hint, or a dropped exception chain, ships without a failing test. The message also says to install the `[spark]` extra for every `ImportError`, including failures that are not a missing PySpark install.
- **Suggested Fix**: Mock the backend import to raise `ImportError` and assert `register_spark()` raises `ImportError` with the install hint and the original exception as `__cause__`.

### Issue 6: Cancellation test still accepts any Exception
- **Severity**: Low
- **File**: test/test_spark.py
- **Line(s)**: 139-141
- **Description**: Pre-existing, not introduced by this diff. `test_job_cancelling` uses bare `pytest.raises(Exception)`. A `TypeError` from a non-iterable error wrapper would still pass, which is the failure mode this release claims to remove. `test_simple` now matches the task message and is the stronger lock. The cancellation test also sleeps 30 seconds and only checks that the temp directory is empty.
- **Risk**: Cancellation can look green when the raised error is the wrong type. The empty directory does not prove the sibling task was cancelled rather than not started.
- **Suggested Fix**: Match `RuntimeError` from the failing task, or otherwise reject a non-iterable-wrapper `TypeError`. Keep this marked pre-existing; the new `test_simple` match should remain the batch-failure lock.

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

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver resolutions

These dispositions supersede the initial Open placeholders. The aggregate Medium
decision and validation results are retained once, in lens 1.

| Issue | Disposition | What, why, and verification |
| --- | --- | --- |
| 1 | Fixed | The fake inheritance wrapper now executes on a real pool thread and records inheritance, active-session setup, and range order. The mapper executes the callable and serialization. Cases cover 4.0.0, 4.0.1, 4.1.0, and 4.3.0 with explicit expected behavior and callback results. Optional pandas cases skip in a classic-only environment. Actual 4.0 Connect remains a mandatory CI release gate. |
| 2 | Fixed | Explicit expected call styles replace the duplicated condition. Tests execute the submitted wrapper, reject the session as the callable argument, and verify both callbacks are forwarded. The real unpinned helper test is retained; its gateway patch is consumed by the public Spark helper, not directly by apply_async. Real pinned classic integration also passed. |
| 3 | Fixed for the changed transport contract | Success and failure callbacks are asserted with the actual result/error object. Driver SystemExit must complete with a chained RuntimeError. Classic mapper errors and managed reuse are exercised by real old/current integration tests. The unchanged cancellation-flag branch was not expanded solely for coverage. |
| 4 | Fixed | Added Python 3.11/joblib 1.0.1/Spark 3.5/Java 17 classic and Python 3.11/joblib 1.3.2/Spark 4.0 Connect to CI. Unsupported combinations remain excluded. |
| 5 | Fixed | Missing root PySpark preserves its cause and gives a managed-runtime warning; a missing packaging module propagates the original ImportError unchanged. Both cases pass. |
| 6 | Tightened | The cancellation test now matches its deliberate task-failure message, preventing unrelated wrapper errors from passing. Proving sibling-start ordering beyond the existing test was not introduced as unrelated timing machinery. |
