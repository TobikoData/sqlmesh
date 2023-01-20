from sqlmesh.core.config import Config, DuckDBConnectionConfig
from sqlmesh.core.notification_target import ConsoleNotificationTarget
from sqlmesh.core.user import User


def test_root_update_with_connections():
    conn0_config = DuckDBConnectionConfig()
    conn1_config = DuckDBConnectionConfig(database="test")

    assert Config(connections=conn0_config).update_with(
        Config(connections={"conn1": conn1_config})
    ) == Config(connections={"": conn0_config, "conn1": conn1_config})

    assert Config(connections={"conn1": conn1_config}).update_with(
        Config(connections=conn0_config)
    ) == Config(connections={"conn1": conn1_config, "": conn0_config})

    assert Config(connections=conn0_config).update_with(
        Config(connections=conn1_config)
    ) == Config(connections=conn1_config)

    assert Config(connections={"conn0": conn0_config}).update_with(
        Config(connections={"conn1": conn1_config})
    ) == Config(connections={"conn0": conn0_config, "conn1": conn1_config})


def test_update_with_users():
    user_a = User(username="a")
    user_b = User(username="b")

    assert Config(users=[user_a]).update_with(Config(users=[user_b])) == Config(
        users=[user_a, user_b]
    )


def test_update_with_ignore_patterns():
    pattern_a = "pattern_a"
    pattern_b = "pattern_b"

    assert Config(ignore_patterns=[pattern_a]).update_with(
        Config(ignore_patterns=[pattern_b])
    ) == Config(ignore_patterns=[pattern_a, pattern_b])


def test_update_with_notification_targets():
    target = ConsoleNotificationTarget()

    assert Config(notification_targets=[target]).update_with(
        Config(notification_targets=[target])
    ) == Config(notification_targets=[target] * 2)
