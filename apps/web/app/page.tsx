"use client";

import React, { useEffect, useState } from "react";


/* global fetch */

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:8000";

type ChatMessage = { role: string; content: string };
type ThreadSummary = { thread_id: string; summary: string };
type ThreadDetail = { thread_id: string; summary: string; recent_messages: ChatMessage[] };
type ChatResponse = { reply?: string; tool_calls?: unknown[]; thread_id?: string };
type GoogleStatus = {
  connected: boolean;
  email?: string;
  scopes?: string[];
  expiry?: number;
};
type BowlingScheduleItem = { date?: string; time?: string | null; lane?: string | null };
type TeamSummary = {
  team_summary?: {
    team?: string;
    position?: number;
    points?: number;
    points_from_first?: number;
    schedule?: BowlingScheduleItem[];
  };
};
type BowlerStatsResponse = { bowlers?: Array<{ bowler?: string; team?: string; average?: number }> };

async function sendChat(message: string, threadId: string) {
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

async function listThreads(): Promise<ThreadSummary[]> {
  const response = await fetch(`${API_BASE_URL}/threads`);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Failed to fetch threads");
  }
  return response.json();
}

async function getThread(threadId: string): Promise<ThreadDetail> {
  const response = await fetch(`${API_BASE_URL}/threads/${threadId}`);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Failed to fetch thread");
  }
  return response.json();
}

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || "Request failed");
  }
  return response.json();
}

function parseNextDate(items: BowlingScheduleItem[]) {
  if (items.length === 0) {
    return null;
  }
  const now = new Date();
  const year = now.getFullYear();
  const parsed = items
    .map((item) => {
      const date = item.date || "";
      const [monthStr, dayStr] = date.split("/");
      const month = Number(monthStr);
      const day = Number(dayStr);
      if (!month || !day) {
        return null;
      }
      const dateObj = new Date(year, month - 1, day);
      return { ...item, dateObj };
    })
    .filter((item): item is BowlingScheduleItem & { dateObj: Date } => !!item);
  parsed.sort((a, b) => a.dateObj.getTime() - b.dateObj.getTime());
  return parsed.find((item) => item.dateObj >= now) || parsed[0] || null;
}

function normalizeBowlerName(raw: string) {
  return raw
    .replace(/'s$/i, "")
    .replace(/[^\w .'-]+$/g, "")
    .trim();
}

async function handleBowlingQuery(message: string) {
  const lower = message.toLowerCase();
  const nextMatch = lower.match(/when does ([a-z0-9 .'-]+) bowl next\??/i);
  if (nextMatch) {
    const target = normalizeBowlerName(nextMatch[1].trim());
    let team = "";
    const stats = await fetchJson<BowlerStatsResponse>(
      `${API_BASE_URL}/api/bowling/casco/monday/bowlers?player_name=${encodeURIComponent(
        target
      )}`
    );
    team = stats.bowlers?.[0]?.team || "";
    if (!team) {
      team = target;
    }
    const summary = await fetchJson<TeamSummary>(
      `${API_BASE_URL}/api/bowling/casco/monday/team-summary?team_name=${encodeURIComponent(
        team
      )}`
    );
    const schedule = summary.team_summary?.schedule || [];
    const next = parseNextDate(schedule);
    if (!next || !next.date || !next.time) {
      return `${team} is in the Monday league, but I couldn't find the next scheduled game.`;
    }
    if (stats.bowlers?.[0]?.bowler) {
      return `${stats.bowlers[0].bowler} bowls for ${team}. Next game: ${next.date} at ${next.time} on lane ${next.lane ?? "TBD"}.`;
    }
    return `${team} bowls next on ${next.date} at ${next.time} on lane ${next.lane ?? "TBD"}.`;
  }

  const avgMatch = lower.match(/what is ([a-z0-9 .'-]+)s average.*monday/i);
  if (avgMatch) {
    const bowler = normalizeBowlerName(avgMatch[1].trim());
    const stats = await fetchJson<BowlerStatsResponse>(
      `${API_BASE_URL}/api/bowling/casco/monday/bowlers?player_name=${encodeURIComponent(
        bowler
      )}`
    );
    const entry = stats.bowlers?.[0];
    if (!entry) {
      return `I couldn't find an average for ${bowler}.`;
    }
    return `${entry.bowler} bowls for ${entry.team}. Current average: ${entry.average ?? "N/A"}.`;
  }

  const bopoMatch = lower.match(/what is ([a-z0-9 .'-]+)s bowling average.*bopo/i);
  if (bopoMatch) {
    const bowler = normalizeBowlerName(bopoMatch[1].trim());
    const stats = await fetchJson<BowlerStatsResponse>(
      `${API_BASE_URL}/api/bowling/bopo/averages?player_name=${encodeURIComponent(
        bowler
      )}`
    );
    const entry = stats.bowlers?.[0];
    if (!entry) {
      return `I couldn't find a BoPo average for ${bowler}.`;
    }
    return `${entry.bowler} bowls for ${entry.team}. BoPo average: ${entry.average ?? "N/A"}.`;
  }

  return null;
}

export default function HomePage() {
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatStatus, setChatStatus] = useState("");
  const [threadId, setThreadId] = useState("");
  const [threads, setThreads] = useState<ThreadSummary[]>([]);
  const [threadsStatus, setThreadsStatus] = useState("");
  const [googleStatus, setGoogleStatus] = useState<GoogleStatus | null>(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/integrations/google/status`)
      .then((response) => response.json())
      .then((data) => setGoogleStatus(data))
      .catch(() => setGoogleStatus({ connected: false }));
  }, []);


  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const message = String(formData.get("message") || "").trim();
    if (!message) {
      return;
    }

    setChatStatus("Sending...");
    setChatMessages((prev) => [...prev, { role: "user", content: message }]);
    try {
      const bowlingReply = await handleBowlingQuery(message);
      if (bowlingReply) {
        setChatMessages((prev) => [
          ...prev,
          { role: "assistant", content: bowlingReply },
        ]);
        setChatStatus("Done");
        return;
      }
      const result = (await sendChat(message, threadId)) as ChatResponse;
      if (result.thread_id) {
        setThreadId(result.thread_id);
      }
      if (result.reply) {
        setChatMessages((prev) => [
          ...prev,
          { role: "assistant", content: result.reply },
        ]);
      }
      if (result.tool_calls && result.tool_calls.length > 0) {
        setChatMessages((prev) => [
          ...prev,
          {
            role: "tool_calls",
            content: JSON.stringify(result.tool_calls, null, 2),
          },
        ]);
      }
    } catch (error) {
      if (error instanceof Error) {
        setChatStatus(error.message);
      } else {
        setChatStatus("Request failed");
      }
      return;
    }

    setChatStatus("Done");
  }

  return (
    <main className="container">
      <header className="header">
        <img src="/logo.svg" alt="Home Ops Copilot logo" width={64} height={64} />
        <h1>Home Ops Copilot</h1>
        <p>Test the `/chat` endpoint and inspect tool calls.</p>
      </header>

      <section className="card">
        <h2>Integrations</h2>
        <div className="integrations">
          <div>
            <strong>Google</strong>
            <div className="meta">
              {googleStatus?.connected
                ? `Connected${googleStatus.email ? `: ${googleStatus.email}` : ""}`
                : "Not connected"}
            </div>
          </div>
          <div className="actions">
            {googleStatus?.connected ? (
              <button
                type="button"
                className="button secondary"
                onClick={async () => {
                  await fetch(`${API_BASE_URL}/api/integrations/google/disconnect`, {
                    method: "POST",
                  });
                  setGoogleStatus({ connected: false });
                }}
              >
                Disconnect
              </button>
            ) : (
              <button
                type="button"
                className="button secondary"
                onClick={() => {
                  window.location.href = `${API_BASE_URL}/api/integrations/google/start`;
                }}
              >
                Connect Google
              </button>
            )}
          </div>
        </div>
      </section>


      <section className="card">
        <h2>Chat</h2>
        <details className="threads-panel">
          <summary className="threads-summary">
            Recent Threads
          </summary>
          <div className="threads-body">
            <div className="threads-header">
              <button
                type="button"
                className="button secondary"
                onClick={async () => {
                  setThreadsStatus("Loading...");
                  try {
                    const data = await listThreads();
                    setThreads(data);
                    setThreadsStatus("Loaded");
                  } catch (error) {
                    if (error instanceof Error) {
                      setThreadsStatus(error.message);
                    } else {
                      setThreadsStatus("Failed to load threads");
                    }
                  }
                }}
              >
                Refresh
              </button>
            </div>
            {threads.length === 0 ? (
              <p className="meta">No threads yet.</p>
            ) : (
              <ul className="threads-list">
                {threads.map((thread) => (
                  <li key={thread.thread_id} className="thread-row">
                    <div className="thread-summary">
                      {thread.summary || "No summary"}
                    </div>
                    <button
                      type="button"
                      className="button secondary"
                      onClick={async () => {
                        setThreadsStatus("Loading thread...");
                        try {
                          const detail = await getThread(thread.thread_id);
                          setThreadId(detail.thread_id);
                          setChatMessages(detail.recent_messages || []);
                          setThreadsStatus("Loaded");
                        } catch (error) {
                        if (error instanceof Error) {
                          setThreadsStatus(error.message);
                        } else {
                          setThreadsStatus("Failed to load thread");
                        }
                        }
                      }}
                    >
                      Load
                    </button>
                  </li>
                ))}
              </ul>
            )}
            <div className="status">{threadsStatus}</div>
          </div>
        </details>

        <div className="transcript">
          {chatMessages.length === 0 ? (
            <p className="meta">No messages yet.</p>
          ) : (
            chatMessages.map((msg, index) => (
              <div className={`message ${msg.role}`} key={`${msg.role}-${index}`}>
                <div className="message-role">{msg.role}</div>
                <pre className="message-content">{msg.content}</pre>
              </div>
            ))
          )}
        </div>

        <form className="chat-form" onSubmit={handleSubmit}>
          <label className="label" htmlFor="threadId">
            Thread ID
          </label>
          <input
            id="threadId"
            name="threadId"
            placeholder="Leave blank to start a new thread"
            className="input"
            value={threadId}
            onChange={(event) => setThreadId(event.target.value)}
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

          <div className="row">
            <button type="submit" className="button">
              Send
            </button>
            <div className="status">{chatStatus}</div>
          </div>
        </form>
      </section>

    </main>
  );
}
