"use client";

import React from "react";

/* global fetch */

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:8000";

async function sendChat(message) {
  const response = await fetch(`${API_BASE_URL}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Request failed");
  }

  return response.json();
}

export default function HomePage() {
  async function handleSubmit(event) {
    event.preventDefault();
    const form = event.currentTarget;
    const message = form.message.value.trim();
    if (!message) {
      return;
    }

    form.status.value = "Sending...";
    try {
      const result = await sendChat(message);
      form.reply.value = result.reply || "";
      form.toolCalls.value = JSON.stringify(result.tool_calls || [], null, 2);
    } catch (error) {
      form.reply.value = "";
      form.toolCalls.value = "";
      form.status.value = error.message;
      return;
    }

    form.status.value = "Done";
  }

  return (
    <main className="container">
      <header className="header">
        <h1>Home Ops Copilot</h1>
        <p>Test the `/chat` endpoint and inspect tool calls.</p>
      </header>

      <form className="card" onSubmit={handleSubmit}>
        <label className="label" htmlFor="message">
          Message
        </label>
        <textarea
          id="message"
          name="message"
          placeholder="Create a groceries list and add milk."
          rows={4}
          className="input"
        />

        <button type="submit" className="button">
          Send
        </button>

        <label className="label" htmlFor="reply">
          Reply
        </label>
        <textarea id="reply" name="reply" readOnly rows={3} className="input" />

        <label className="label" htmlFor="toolCalls">
          Tool Calls
        </label>
        <textarea
          id="toolCalls"
          name="toolCalls"
          readOnly
          rows={10}
          className="input"
        />

        <label className="label" htmlFor="status">
          Status
        </label>
        <input id="status" name="status" readOnly className="input" />
      </form>
    </main>
  );
}
