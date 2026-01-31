"use client";

import React, { useEffect, useState } from "react";

import { logger } from "./logger";

/* global fetch */

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:8000";

async function sendChat(message, threadId) {
  const response = await fetch(`${API_BASE_URL}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message, thread_id: threadId || null }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Request failed");
  }

  return response.json();
}

async function listReminders() {
  const response = await fetch(`${API_BASE_URL}/reminders`);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Failed to fetch reminders");
  }
  return response.json();
}

async function createReminder(payload) {
  const response = await fetch(`${API_BASE_URL}/reminders`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Failed to create reminder");
  }
  return response.json();
}

async function updateReminder(id, payload) {
  const response = await fetch(`${API_BASE_URL}/reminders/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Failed to update reminder");
  }
  return response.json();
}

async function completeReminder(id) {
  const response = await fetch(`${API_BASE_URL}/reminders/${id}/complete`, {
    method: "POST",
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Failed to complete reminder");
  }
  return response.json();
}

async function deleteReminder(id) {
  const response = await fetch(`${API_BASE_URL}/reminders/${id}`, {
    method: "DELETE",
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Failed to delete reminder");
  }
  return response.json();
}

export default function HomePage() {
  const [reminders, setReminders] = useState([]);
  const [reminderStatus, setReminderStatus] = useState("");
  const [editingId, setEditingId] = useState(null);
  const [formValues, setFormValues] = useState({
    title: "",
    description: "",
    cron: "0 9 * * 1",
    timezone: "UTC",
    email: "",
    sms_phone: "",
    sms_gateway_domain: "",
  });

  useEffect(() => {
    let mounted = true;
    listReminders()
      .then((data) => {
        if (mounted) {
          setReminders(data);
        }
      })
      .catch((error) => {
        if (mounted) {
          logger.error("reminders_fetch_failed", error.message);
          setReminderStatus(error.message);
        }
      });
    return () => {
      mounted = false;
    };
  }, []);

  function handleReminderChange(event) {
    const { name, value } = event.target;
    setFormValues((prev) => ({ ...prev, [name]: value }));
  }

  async function handleReminderSubmit(event) {
    event.preventDefault();
    setReminderStatus("Saving...");
    try {
      if (editingId) {
        await updateReminder(editingId, formValues);
      } else {
        await createReminder(formValues);
      }
      const data = await listReminders();
      setReminders(data);
      setEditingId(null);
      setFormValues({
        title: "",
        description: "",
        cron: "0 9 * * 1",
        timezone: "UTC",
        email: "",
        sms_phone: "",
        sms_gateway_domain: "",
      });
      setReminderStatus("Saved");
    } catch (error) {
      logger.error("reminders_save_failed", error.message);
      setReminderStatus(error.message);
    }
  }

  async function handleReminderEdit(reminder) {
    setEditingId(reminder.id);
    setFormValues({
      title: reminder.title || "",
      description: reminder.description || "",
      cron: reminder.cron || "",
      timezone: reminder.timezone || "UTC",
      email: reminder.email || "",
      sms_phone: reminder.sms_phone || "",
      sms_gateway_domain: reminder.sms_gateway_domain || "",
    });
  }

  async function handleReminderComplete(id) {
    setReminderStatus("Completing...");
    try {
      await completeReminder(id);
      const data = await listReminders();
      setReminders(data);
      setReminderStatus("Completed");
    } catch (error) {
      logger.error("reminders_complete_failed", error.message);
      setReminderStatus(error.message);
    }
  }

  async function handleReminderDelete(id) {
    setReminderStatus("Deleting...");
    try {
      await deleteReminder(id);
      const data = await listReminders();
      setReminders(data);
      setReminderStatus("Deleted");
    } catch (error) {
      logger.error("reminders_delete_failed", error.message);
      setReminderStatus(error.message);
    }
  }
  async function handleSubmit(event) {
    event.preventDefault();
    const form = event.currentTarget;
    const message = form.message.value.trim();
    const threadId = form.threadId.value.trim();
    if (!message) {
      return;
    }

    form.status.value = "Sending...";
    try {
      const result = await sendChat(message, threadId);
      form.reply.value = result.reply || "";
      form.toolCalls.value = JSON.stringify(result.tool_calls || [], null, 2);
      if (result.thread_id) {
        form.threadId.value = result.thread_id;
      }
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
        <p>
          Reminders live at <code>/reminders</code> and are managed via the API
          for now.
        </p>
        <p>
          Calendar endpoints: <code>/calendar/events</code> and{" "}
          <code>/calendar/events/&#123;id&#125;</code>.
        </p>
      </header>

      <form className="card" onSubmit={handleSubmit}>
        <label className="label" htmlFor="threadId">
          Thread ID
        </label>
        <input
          id="threadId"
          name="threadId"
          placeholder="Leave blank to start a new thread"
          className="input"
        />

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

      <section className="card">
        <h2>Reminders</h2>
        <form className="reminder-form" onSubmit={handleReminderSubmit}>
          <div className="row">
            <label className="label" htmlFor="title">
              Title
            </label>
            <input
              id="title"
              name="title"
              value={formValues.title}
              onChange={handleReminderChange}
              className="input"
              required
            />
          </div>
          <div className="row">
            <label className="label" htmlFor="description">
              Description
            </label>
            <input
              id="description"
              name="description"
              value={formValues.description}
              onChange={handleReminderChange}
              className="input"
            />
          </div>
          <div className="row">
            <label className="label" htmlFor="cron">
              Cron
            </label>
            <input
              id="cron"
              name="cron"
              value={formValues.cron}
              onChange={handleReminderChange}
              className="input"
              required
            />
          </div>
          <div className="row">
            <label className="label" htmlFor="timezone">
              Timezone
            </label>
            <input
              id="timezone"
              name="timezone"
              value={formValues.timezone}
              onChange={handleReminderChange}
              className="input"
              required
            />
          </div>
          <div className="row">
            <label className="label" htmlFor="email">
              Email
            </label>
            <input
              id="email"
              name="email"
              value={formValues.email}
              onChange={handleReminderChange}
              className="input"
            />
          </div>
          <div className="row">
            <label className="label" htmlFor="sms_phone">
              SMS Phone
            </label>
            <input
              id="sms_phone"
              name="sms_phone"
              value={formValues.sms_phone}
              onChange={handleReminderChange}
              className="input"
            />
          </div>
          <div className="row">
            <label className="label" htmlFor="sms_gateway_domain">
              SMS Gateway Domain
            </label>
            <input
              id="sms_gateway_domain"
              name="sms_gateway_domain"
              value={formValues.sms_gateway_domain}
              onChange={handleReminderChange}
              className="input"
            />
          </div>
          <div className="row">
            <button type="submit" className="button">
              {editingId ? "Update Reminder" : "Create Reminder"}
            </button>
            {editingId && (
              <button
                type="button"
                className="button secondary"
                onClick={() => {
                  setEditingId(null);
                  setFormValues({
                    title: "",
                    description: "",
                    cron: "0 9 * * 1",
                    timezone: "UTC",
                    email: "",
                    sms_phone: "",
                    sms_gateway_domain: "",
                  });
                }}
              >
                Cancel
              </button>
            )}
          </div>
          <div className="status">{reminderStatus}</div>
        </form>

        <div className="reminders-list">
          {reminders.length === 0 ? (
            <p>No reminders yet.</p>
          ) : (
            reminders.map((reminder) => (
              <div className="reminder-row" key={reminder.id}>
                <div>
                  <strong>{reminder.title}</strong>
                  <div className="meta">
                    {reminder.cron} · {reminder.timezone}
                  </div>
                  {reminder.email && <div className="meta">Email: {reminder.email}</div>}
                  {reminder.sms_phone && reminder.sms_gateway_domain && (
                    <div className="meta">
                      SMS: {reminder.sms_phone}@{reminder.sms_gateway_domain}
                    </div>
                  )}
                </div>
                <div className="actions">
                  <button
                    type="button"
                    className="button secondary"
                    onClick={() => handleReminderEdit(reminder)}
                  >
                    Edit
                  </button>
                  <button
                    type="button"
                    className="button secondary"
                    onClick={() => handleReminderComplete(reminder.id)}
                    disabled={!reminder.active}
                  >
                    Complete
                  </button>
                  <button
                    type="button"
                    className="button danger"
                    onClick={() => handleReminderDelete(reminder.id)}
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))
          )}
        </div>
      </section>
    </main>
  );
}
