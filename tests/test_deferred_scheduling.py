"""Deferred scheduling tests — one-time scheduledStartTime and recurring cron schedule.

Tests cover:
  - Scheduled jobs entering Scheduled phase and transitioning after the time is reached
  - Past scheduledStartTime starting immediately
  - Invalid scheduledStartTime failing the job
  - Mutual exclusivity of schedule and scheduledStartTime
  - Invalid cron expression failing the job
  - Recurring jobs entering Recurring phase and creating children
  - trigger-now annotation creating a child immediately
  - Backward compatibility: no scheduling fields behaves as before
"""

import subprocess
import time
from datetime import UTC, datetime, timedelta

import pytest
from kubernetes.client.exceptions import ApiException

from fournos.core.constants import LABEL_RECURRING_PARENT, Phase
from tests.conftest import (
    GROUP,
    NAMESPACE,
    PLURAL,
    VERSION,
    create_job,
    get_job,
    job_status_summary,
    poll_phase,
)


def _spec(**overrides) -> dict:
    """Build a clusterless base spec, merging in *overrides*."""
    base = {
        "clusterless": True,
        "exclusive": False,
        "executionEngine": {
            "forge": {
                "project": "skeleton",
                "args": [],
            }
        },
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Validation tests (fast — no waiting for timers)
# ---------------------------------------------------------------------------


def test_invalid_scheduled_start_time(k8s):
    """Invalid scheduledStartTime format → rejected by CRD validation (422)."""
    with pytest.raises(ApiException) as exc_info:
        create_job(k8s, "test-bad-timestamp", _spec(scheduledStartTime="not-a-date"))

    assert exc_info.value.status == 422, (
        f"Expected 422 Unprocessable Entity, got {exc_info.value.status}"
    )
    assert "scheduledStartTime" in (exc_info.value.body or ""), (
        f"Error body should mention scheduledStartTime, got: {exc_info.value.body!r}"
    )


def test_invalid_cron_expression(k8s):
    """Invalid cron expression → immediate Failed."""
    create_job(k8s, "test-bad-cron", _spec(schedule="not a cron"))

    phase = poll_phase(
        k8s,
        "test-bad-cron",
        terminal={Phase.FAILED},
        timeout=15,
    )
    assert phase == Phase.FAILED, job_status_summary(k8s, "test-bad-cron")

    job = get_job(k8s, "test-bad-cron")
    msg = job["status"]["message"].lower()
    assert "invalid" in msg and "cron" in msg, (
        f"Failure message should mention invalid cron, got: {msg!r}"
    )


def test_schedule_and_scheduled_start_time_mutual_exclusion(k8s):
    """Setting both schedule and scheduledStartTime → immediate Failed."""
    future = (datetime.now(UTC) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    create_job(
        k8s,
        "test-mutual-exclusive",
        _spec(schedule="0 20 * * *", scheduledStartTime=future),
    )

    phase = poll_phase(
        k8s,
        "test-mutual-exclusive",
        terminal={Phase.FAILED},
        timeout=15,
    )
    assert phase == Phase.FAILED, job_status_summary(k8s, "test-mutual-exclusive")

    job = get_job(k8s, "test-mutual-exclusive")
    msg = job["status"]["message"].lower()
    assert "mutually exclusive" in msg, (
        f"Failure message should mention 'mutually exclusive', got: {msg!r}"
    )


def test_no_scheduling_fields_backward_compat(k8s):
    """Job without scheduledStartTime or schedule enters the normal lifecycle."""
    create_job(k8s, "test-no-sched", _spec())

    phase = poll_phase(
        k8s,
        "test-no-sched",
        terminal={Phase.RESOLVING, Phase.ADMITTED, Phase.RUNNING, Phase.SUCCEEDED},
        timeout=30,
    )
    assert phase != Phase.SCHEDULED, (
        f"Job without scheduling fields should not enter Scheduled phase, got {phase!r}"
    )
    assert phase != Phase.RECURRING, (
        f"Job without scheduling fields should not enter Recurring phase, got {phase!r}"
    )


# ---------------------------------------------------------------------------
# Scheduled start time tests
# ---------------------------------------------------------------------------


def test_scheduled_start_time_future(k8s):
    """Job with future scheduledStartTime enters Scheduled phase, then transitions."""
    start = (datetime.now(UTC) + timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    create_job(k8s, "test-sched-future", _spec(scheduledStartTime=start))

    phase = poll_phase(
        k8s,
        "test-sched-future",
        terminal={Phase.SCHEDULED},
        timeout=15,
    )
    assert phase == Phase.SCHEDULED, job_status_summary(k8s, "test-sched-future")

    job = get_job(k8s, "test-sched-future")
    assert "scheduled" in job["status"]["message"].lower(), (
        f"Scheduled job should have schedule message, got: {job['status']['message']!r}"
    )

    phase = poll_phase(
        k8s,
        "test-sched-future",
        terminal={
            Phase.RESOLVING,
            Phase.ADMITTED,
            Phase.RUNNING,
            Phase.SUCCEEDED,
            Phase.FAILED,
        },
        timeout=60,
    )
    assert phase != Phase.SCHEDULED, (
        f"Job should have left Scheduled phase after start time, got {phase!r}"
    )


def test_scheduled_start_time_past(k8s):
    """Job with past scheduledStartTime skips Scheduled phase and starts immediately."""
    past = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    create_job(k8s, "test-sched-past", _spec(scheduledStartTime=past))

    phase = poll_phase(
        k8s,
        "test-sched-past",
        terminal={
            Phase.RESOLVING,
            Phase.ADMITTED,
            Phase.RUNNING,
            Phase.SUCCEEDED,
            Phase.FAILED,
        },
        timeout=30,
    )
    assert phase != Phase.SCHEDULED, (
        f"Past scheduledStartTime should skip Scheduled phase, got {phase!r}"
    )


# ---------------------------------------------------------------------------
# Recurring schedule tests
# ---------------------------------------------------------------------------


def test_recurring_enters_recurring_phase(k8s):
    """Job with valid cron expression enters Recurring phase."""
    create_job(k8s, "test-recurring-phase", _spec(schedule="0 0 1 1 *"))

    phase = poll_phase(
        k8s,
        "test-recurring-phase",
        terminal={Phase.RECURRING},
        timeout=15,
    )
    assert phase == Phase.RECURRING, job_status_summary(k8s, "test-recurring-phase")

    job = get_job(k8s, "test-recurring-phase")
    msg = job["status"]["message"]
    assert "recurring" in msg.lower(), (
        f"Recurring job message should mention 'recurring', got: {msg!r}"
    )


def test_trigger_now_creates_child(k8s):
    """Setting fournos.dev/trigger-now=true creates a child FournosJob immediately."""
    create_job(k8s, "test-trigger-now", _spec(schedule="0 0 1 1 *"))

    poll_phase(
        k8s,
        "test-trigger-now",
        terminal={Phase.RECURRING},
        timeout=15,
    )

    subprocess.run(
        [
            "kubectl",
            "annotate",
            "fjob",
            "test-trigger-now",
            "fournos.dev/trigger-now=true",
            "-n",
            NAMESPACE,
            "--overwrite",
        ],
        check=True,
        capture_output=True,
    )

    deadline = time.monotonic() + 30
    children = []
    while time.monotonic() < deadline:
        jobs = k8s.list_namespaced_custom_object(GROUP, VERSION, NAMESPACE, PLURAL)
        children = [
            j
            for j in jobs["items"]
            if j.get("metadata", {}).get("labels", {}).get(LABEL_RECURRING_PARENT)
            == "test-trigger-now"
        ]
        if children:
            break
        time.sleep(2)

    assert len(children) >= 1, (
        "trigger-now should have created at least one child FournosJob"
    )

    child = children[0]
    child_spec = child["spec"]
    assert "schedule" not in child_spec, (
        "Child job should not inherit the 'schedule' field"
    )
    assert "scheduledStartTime" not in child_spec, (
        "Child job should not inherit the 'scheduledStartTime' field"
    )

    parent = get_job(k8s, "test-trigger-now")
    assert parent["status"].get("lastScheduledTime"), (
        "Parent job should have lastScheduledTime set after triggering"
    )
    parent_annotations = parent.get("metadata", {}).get("annotations", {})
    assert parent_annotations.get("fournos.dev/trigger-now") != "true", (
        "trigger-now annotation should be reset after triggering"
    )


def test_recurring_child_labeled_correctly(k8s):
    """Child jobs from recurring parent carry the fournos.dev/recurring-parent label."""
    create_job(k8s, "test-child-label", _spec(schedule="0 0 1 1 *"))

    poll_phase(
        k8s,
        "test-child-label",
        terminal={Phase.RECURRING},
        timeout=15,
    )

    subprocess.run(
        [
            "kubectl",
            "annotate",
            "fjob",
            "test-child-label",
            "fournos.dev/trigger-now=true",
            "-n",
            NAMESPACE,
            "--overwrite",
        ],
        check=True,
        capture_output=True,
    )

    deadline = time.monotonic() + 30
    children = []
    while time.monotonic() < deadline:
        jobs = k8s.list_namespaced_custom_object(
            GROUP,
            VERSION,
            NAMESPACE,
            PLURAL,
            label_selector=f"{LABEL_RECURRING_PARENT}=test-child-label",
        )
        children = jobs.get("items", [])
        if children:
            break
        time.sleep(2)

    assert len(children) >= 1, "Should be able to find children via label selector"
    child_name = children[0]["metadata"]["name"]
    assert child_name.startswith("test-child-label-"), (
        f"Child name should start with parent name prefix, got {child_name!r}"
    )
