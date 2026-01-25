from packages.core.reminders.service import create_reminder, update_reminder
from packages.core.storage.sqlite import SQLiteListStore


def test_reminder_create_and_update(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))

    reminder = create_reminder(
        store=store,
        title="Trash day",
        description="Take out bins",
        cron="0 9 * * 1",
        timezone="UTC",
        email="test@example.com",
        sms_phone="5551234567",
        sms_gateway_domain="sms.example.com",
    )
    assert reminder.id
    assert reminder.active is True
    assert reminder.next_run_at

    updated = update_reminder(
        store=store,
        reminder=reminder,
        title="Trash day updated",
        cron="0 10 * * 1",
    )
    assert updated.title == "Trash day updated"
    assert updated.cron == "0 10 * * 1"
