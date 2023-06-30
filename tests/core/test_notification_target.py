from __future__ import annotations

import typing as t
from unittest import mock

import pytest

from sqlmesh.core.notification_target import (
    ConsoleNotificationTarget,
    NotificationEvent,
    NotificationStatus,
    NotificationTargetManager,
)


@pytest.fixture
def notification_target_manager_with_spy(mocker) -> tuple[NotificationTargetManager, t.Callable]:
    console_notification_target_send_spy = mocker.spy(ConsoleNotificationTarget, "send")
    console_notification_target = ConsoleNotificationTarget()
    test_user_console_notification_target = ConsoleNotificationTarget(
        notify_on={NotificationEvent.APPLY_START}
    )
    notification_target_manager = NotificationTargetManager(
        notification_targets={
            NotificationEvent.APPLY_START: {console_notification_target},
            NotificationEvent.APPLY_END: {console_notification_target},
        },
        user_notification_targets={
            "test_user": {test_user_console_notification_target},
        },
    )
    return notification_target_manager, console_notification_target_send_spy


def test_notify(notification_target_manager_with_spy):
    notification_target_manager, spy = notification_target_manager_with_spy
    notification_target_manager.notify(NotificationEvent.APPLY_START, "prod")
    spy.assert_called_once_with(
        mock.ANY,
        NotificationStatus.INFO,
        "Plan apply started for environment `prod`.",
    )

    spy.reset_mock()
    notification_target_manager.notify(NotificationEvent.APPLY_END, "prod")
    spy.assert_called_once_with(
        mock.ANY,
        NotificationStatus.SUCCESS,
        "Plan apply finished for environment `prod`.",
    )

    # No notification target configured for APPLY_FAILURE event
    spy.reset_mock()
    notification_target_manager.notify(NotificationEvent.APPLY_FAILURE, ValueError())
    spy.assert_not_called()


def test_notify_user(notification_target_manager_with_spy):
    notification_target_manager, spy = notification_target_manager_with_spy
    notification_target_manager.notify_user(NotificationEvent.APPLY_START, "test_user", "prod")
    spy.assert_called_once_with(
        mock.ANY,
        NotificationStatus.INFO,
        "Plan apply started for environment `prod`.",
    )

    # No notification target configured for APPLY_END event for test_user
    spy.reset_mock()
    notification_target_manager.notify_user(NotificationEvent.APPLY_END, "test_user", "prod")
    spy.assert_not_called()
