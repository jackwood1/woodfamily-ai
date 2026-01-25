from packages.core.storage.sqlite import SQLiteListStore


def test_thread_store_create_get_update(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))

    thread_id = store.create_thread()
    assert thread_id

    state = store.get_thread(thread_id)
    assert state is not None
    assert state.thread_id == thread_id
    assert state.summary == ""
    assert state.recent_messages == []

    store.update_thread(
        thread_id,
        "Recent: hi -> hello",
        [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"}],
    )

    updated = store.get_thread(thread_id)
    assert updated is not None
    assert updated.summary == "Recent: hi -> hello"
    assert updated.recent_messages[0]["role"] == "user"
