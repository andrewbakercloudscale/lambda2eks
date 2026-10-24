import pytest

from lambda2eks.schedule import ScheduleError, to_k8s_cron


@pytest.mark.parametrize("expr, cron", [
    ("rate(1 minute)", "* * * * *"),
    ("rate(5 minutes)", "*/5 * * * *"),
    ("rate(1 hour)", "0 * * * *"),
    ("rate(6 hours)", "0 */6 * * *"),
    ("rate(1 day)", "0 0 * * *"),
    ("cron(30 2 ? * MON-FRI *)", "30 2 * * MON-FRI"),
    ("cron(0 12 * * ? *)", "0 12 * * *"),
    ("cron(0 8 ? * 2-6 *)", "0 8 * * 1-5"),      # EventBridge 1=SUN, Kubernetes 0=SUN
    ("cron(0 0 ? * 1,7 *)", "0 0 * * 0,6"),
])
def test_conversions(expr, cron):
    assert to_k8s_cron(expr)[0] == cron


def test_warnings():
    assert to_k8s_cron("rate(7 minutes)")[1]
    assert to_k8s_cron("cron(0 0 L * ? *)")[1]
    assert to_k8s_cron("cron(0 0 * * ? 2027)")[1]


@pytest.mark.parametrize("bad", ["every day", "cron(0 0 * *)", "cron(0 0 ? * 8 *)"])
def test_errors(bad):
    with pytest.raises(ScheduleError):
        to_k8s_cron(bad)
