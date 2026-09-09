"""Tests for sky.jobs.server.core."""
from unittest import mock

import pytest

from sky import backends
from sky.jobs.server import core as jobs_core


def _forwarded_tail(tail):
    """Call ``jobs_core.tail_logs`` with ``tail`` (mocking out the controller
    restart / backend / runner) and return the ``tail`` value forwarded to
    ``tail_managed_job_logs``."""
    fake_backend = mock.MagicMock(spec=backends.CloudVmRayBackend)
    fake_runner = mock.MagicMock()
    fake_runner.tail_managed_job_logs.return_value = 0
    with mock.patch.object(jobs_core, '_maybe_restart_controller',
                           return_value=mock.MagicMock()), \
         mock.patch.object(jobs_core.backend_utils,
                           'get_backend_from_handle',
                           return_value=fake_backend), \
         mock.patch.object(jobs_core.managed_job_runner,
                           'current',
                           return_value=fake_runner):
        jobs_core.tail_logs(name=None,
                            job_id=1,
                            follow=False,
                            controller=False,
                            refresh=False,
                            tail=tail)
    fake_runner.tail_managed_job_logs.assert_called_once()
    return fake_runner.tail_managed_job_logs.call_args.kwargs['tail']


@pytest.mark.parametrize(
    ('given', 'expected'),
    [
        (0, None),  # dashboard download button's "all lines" sentinel
        (-1, None),  # `sky jobs logs --tail -1`
        (None, None),  # no tail -> all
        (200, 200),  # positive tail forwarded unchanged
        (5000, 5000),
    ])
def test_tail_logs_normalizes_non_positive_tail(given, expected):
    """A non-positive tail (0 / -1) means "all lines" and must be normalized
    to None before reaching the backward-seek tail reader (which asserts
    tail > 0). Otherwise the dashboard download (tail=0) raises
    AssertionError and produces an empty log."""
    assert _forwarded_tail(given) == expected


class TestValidateParentJob:
    """`launch(parent_job_id=...)` pre-checks in consolidation mode."""

    @staticmethod
    def _run(parent_job_id,
             parent_task_id=None,
             *,
             consolidation=True,
             status=None,
             parent_workspace='default',
             active_workspace='default'):
        status_enum = (None if status is None else
                       jobs_core.managed_job_state.ManagedJobStatus(status))
        rows = ([{
            'job_id': parent_job_id,
            'workspace': parent_workspace
        }] if status is not None else [])
        with mock.patch.object(jobs_core.managed_job_utils,
                               'is_consolidation_mode',
                               return_value=consolidation), \
             mock.patch.object(jobs_core.managed_job_state, 'get_status',
                               return_value=status_enum), \
             mock.patch.object(jobs_core.managed_job_state,
                               'get_managed_jobs_with_filters',
                               return_value=(rows, len(rows))), \
             mock.patch.object(jobs_core.skypilot_config,
                               'get_active_workspace',
                               return_value=active_workspace):
            jobs_core._validate_parent_job(parent_job_id, parent_task_id)

    def test_no_parent_is_a_no_op(self):
        self._run(None)

    def test_task_without_job_rejected(self):
        with pytest.raises(ValueError, match='requires parent_job_id'):
            self._run(None, parent_task_id=1)

    def test_running_parent_in_same_workspace_ok(self):
        self._run(42, 1, status='RUNNING')

    def test_terminal_but_not_cancelled_parent_ok(self):
        # Attaching to a finished group is allowed (visibility still useful).
        self._run(42, status='SUCCEEDED')

    @pytest.mark.parametrize('status', ['CANCELLING', 'CANCELLED'])
    def test_cancelling_or_cancelled_parent_rejected(self, status):
        with pytest.raises(ValueError, match=status):
            self._run(42, status=status)

    def test_missing_parent_rejected(self):
        with pytest.raises(ValueError, match='no such managed job'):
            self._run(42, status=None)

    def test_other_workspace_rejected(self):
        with pytest.raises(ValueError, match='workspace'):
            self._run(42,
                      status='RUNNING',
                      parent_workspace='team-a',
                      active_workspace='team-b')

    def test_remote_controller_skips_checks(self):
        # Not consolidation mode: nothing to check against locally.
        self._run(42, status=None, consolidation=False)
