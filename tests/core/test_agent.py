import json

from packages.core.agent import HomeOpsAgent
from packages.core.storage.sqlite import SQLiteListStore


class FakeLLM:
    def __init__(self):
        self.calls = 0

    def chat(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "type": "function",
                                    "function": {
                                        "name": "create_list",
                                        "arguments": json.dumps({"name": "groceries"}),
                                    },
                                }
                            ]
                        }
                    }
                ]
            }
        if self.calls == 2:
            return {
                "choices": [
                    {
                        "message": {
                            "tool_calls": [
                                {
                                    "id": "call_2",
                                    "type": "function",
                                    "function": {
                                        "name": "add_item",
                                        "arguments": json.dumps(
                                            {"list_name": "groceries", "item": "milk"}
                                        ),
                                    },
                                }
                            ]
                        }
                    }
                ]
            }
        return {
            "choices": [
                {
                    "message": {
                        "content": "Added milk to groceries.",
                    }
                }
            ]
        }


def test_agent_tool_loop(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    agent = HomeOpsAgent(store=store, llm=FakeLLM())

    result = agent.chat("Please add milk to my groceries list.")
    assert result["reply"] == "Added milk to groceries."
    assert len(result["tool_calls"]) == 2
    assert result["tool_calls"][0]["name"] == "create_list"
    assert result["tool_calls"][1]["name"] == "add_item"
    assert result["thread_id"]
