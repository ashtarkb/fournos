"""Cluster lock expiry tests — lockOnly jobs with a lockUntil deadline.

A lockOnly job normally holds a cluster's slots until deleted or shut down
by hand.  lockUntil is an absolute UTC timestamp (same style as
scheduledStartTime) — the reconcile loop auto-releases the lock (same code
path as a user-triggered shutdown) once that time passes.
"""

from datetime import UTC, datetime, timedelta

import pytest
from kubernetes.client.exceptions import ApiException

from fournos.core.constants import Phase
from tests.conftest import (
    create_job,
    get_job,
    job_status_summary,
    poll_phase,
    workload_exists,
)


def _future(seconds: float) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def test_lock_until_without_lock_only_fails(k8s):
    """lockUntil set without lockOnly: true is rejected."""
    create_job(
        k8s,
        "test-lockuntil-no-lockonly",
        {
            "cluster": "cluster-2",
            "exclusive": True,
            "lockUntil": _future(3600),
        },
    )

    phase = poll_phase(
        k8s,
        "test-lockuntil-no-lockonly",
        terminal={Phase.FAILED},
        timeout=15,
    )
    assert phase == Phase.FAILED, job_status_summary(k8s, "test-lockuntil-no-lockonly")

    job = get_job(k8s, "test-lockuntil-no-lockonly")
    assert "lockUntil" in job["status"]["message"]


def test_invalid_lock_until_rejected_by_crd(k8s):
    """Malformed lockUntil is rejected by CRD schema validation (422)."""
    with pytest.raises(ApiException) as exc_info:
        create_job(
            k8s,
            "test-lockuntil-bad-format",
            {
                "cluster": "cluster-2",
                "exclusive": True,
                "lockOnly": True,
                "lockUntil": "not-a-date",
            },
        )
    assert exc_info.value.status == 422


def test_lock_auto_releases_at_lock_until(k8s):
    """A lockOnly job with a near-future lockUntil self-releases and frees the cluster."""
    create_job(
        k8s,
        "test-lock-ttl",
        {
            "cluster": "cluster-2",
            "exclusive": True,
            "lockOnly": True,
            "lockUntil": _future(10),
        },
    )

    phase = poll_phase(
        k8s,
        "test-lock-ttl",
        terminal={Phase.ADMITTED},
        timeout=30,
    )
    assert phase == Phase.ADMITTED, job_status_summary(k8s, "test-lock-ttl")
    assert workload_exists("test-lock-ttl"), "Lock Workload should exist while held"

    phase = poll_phase(
        k8s,
        "test-lock-ttl",
        terminal={Phase.STOPPED},
        timeout=45,
    )
    assert phase == Phase.STOPPED, job_status_summary(k8s, "test-lock-ttl")
    assert not workload_exists("test-lock-ttl"), (
        "Lock Workload should be deleted once lockUntil is reached"
    )


def test_lock_without_lock_until_holds_indefinitely(k8s):
    """A lockOnly job with no lockUntil stays Admitted."""
    create_job(
        k8s,
        "test-lock-no-ttl",
        {"cluster": "cluster-3", "exclusive": True, "lockOnly": True},
    )

    phase = poll_phase(
        k8s,
        "test-lock-no-ttl",
        terminal={Phase.ADMITTED},
        timeout=30,
    )
    assert phase == Phase.ADMITTED, job_status_summary(k8s, "test-lock-no-ttl")

    phase = poll_phase(
        k8s,
        "test-lock-no-ttl",
        terminal={Phase.STOPPED},
        timeout=15,
        raise_on_timeout=False,
    )
    assert phase == Phase.ADMITTED, (
        f"Lock without lockUntil should still be held, got phase={phase!r}"
    )


def test_lock_until_already_past_self_releases_quickly(k8s):
    """A lockOnly job created with a lockUntil already in the past is admitted
    and then released on the very next reconcile tick, rather than being
    rejected outright at creation time.
    """
    create_job(
        k8s,
        "test-lock-past-ttl",
        {
            "cluster": "cluster-2",
            "exclusive": True,
            "lockOnly": True,
            "lockUntil": _future(-3600),
        },
    )

    phase = poll_phase(
        k8s,
        "test-lock-past-ttl",
        terminal={Phase.STOPPED, Phase.FAILED},
        timeout=30,
    )
    assert phase == Phase.STOPPED, job_status_summary(k8s, "test-lock-past-ttl")
    assert not workload_exists("test-lock-past-ttl"), (
        "Lock Workload should be deleted once the (already-past) lockUntil is reached"
    )


def test_lock_until_valid_format_but_naive_datetime_treated_as_utc(k8s):
    """A lockUntil without an explicit 'Z'/offset is treated as UTC (same
    convention as scheduledStartTime), not rejected and not interpreted in
    local time.
    """
    naive_past = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
    create_job(
        k8s,
        "test-lock-naive-ttl",
        {
            "cluster": "cluster-3",
            "exclusive": True,
            "lockOnly": True,
            "lockUntil": naive_past,
        },
    )

    phase = poll_phase(
        k8s,
        "test-lock-naive-ttl",
        terminal={Phase.STOPPED, Phase.FAILED},
        timeout=30,
    )
    assert phase == Phase.STOPPED, job_status_summary(k8s, "test-lock-naive-ttl")
