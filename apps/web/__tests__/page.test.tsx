import React from "react";
import { act, fireEvent, render, screen } from "@testing-library/react";

import HomePage from "../app/page";

describe("HomePage", () => {
  beforeEach(() => {
    global.fetch = jest.fn((input: RequestInfo) => {
      const url = typeof input === "string" ? input : input.url;
      if (url.includes("/api/integrations/google/status")) {
        return Promise.resolve({
          ok: true,
          json: async () => ({ connected: false }),
        });
      }
      if (url.includes("/threads")) {
        return Promise.resolve({
          ok: true,
          json: async () => [],
        });
      }
      if (url.includes("/api/bowling/leagues")) {
        return Promise.resolve({
          ok: true,
          json: async () => [{ key: "monday_bayside", name: "Monday at Bayside" }],
        });
      }
      if (url.includes("/api/bowling/monday_bayside/sync")) {
        return Promise.resolve({
          ok: true,
          json: async () => ({ status: "ok", stats_rows: 1, matches: 2 }),
        });
      }
      if (url.includes("/api/bowling/monday_bayside/teams")) {
        return Promise.resolve({
          ok: true,
          json: async () => [{ name: "Beer Frame" }],
        });
      }
      if (url.includes("/api/bowling/monday_bayside/team-stats")) {
        return Promise.resolve({
          ok: true,
          json: async () => [{ player_name: "Gino", average: 180 }],
        });
      }
      if (url.includes("/api/bowling/monday_bayside/player-stats")) {
        return Promise.resolve({
          ok: true,
          json: async () => [{ player_name: "Gino", average: 180 }],
        });
      }
      if (url.includes("/api/bowling/monday_bayside/matches")) {
        return Promise.resolve({
          ok: true,
          json: async () => [{ lane: "12", team_a: "Beer Frame" }],
        });
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({ reply: "ok", tool_calls: [], thread_id: "t1" }),
      });
    }) as jest.Mock;
  });

  it("renders the heading", async () => {
    await act(async () => {
      render(<HomePage />);
    });
    expect(screen.getByText("Home Ops Copilot")).toBeInTheDocument();
    await screen.findByText("No messages yet.");
    await screen.findByText("Not connected");
  });

  it("renders chat section", async () => {
    await act(async () => {
      render(<HomePage />);
    });
    expect(screen.getByText("Chat")).toBeInTheDocument();
    await screen.findByText("No messages yet.");
  });

  it("renders threads panel", async () => {
    await act(async () => {
      render(<HomePage />);
    });
    expect(screen.getByText("Recent Threads")).toBeInTheDocument();
    await screen.findByText("No threads yet.");
  });

  it("renders integrations card", async () => {
    await act(async () => {
      render(<HomePage />);
    });
    expect(screen.getByText("Integrations")).toBeInTheDocument();
  });

  it("renders bowling section with leagues", async () => {
    await act(async () => {
      render(<HomePage />);
    });
    expect(screen.getByText("Bowling")).toBeInTheDocument();
    await screen.findByText("Monday at Bayside");
  });

  it("loads bowling teams", async () => {
    await act(async () => {
      render(<HomePage />);
    });
    const button = await screen.findByRole("button", { name: "List Teams" });
    await act(async () => {
      fireEvent.click(button);
    });
    await screen.findByText(/Beer Frame/);
  });

  it("sends a message and appends reply", async () => {
    await act(async () => {
      render(<HomePage />);
    });
    const textarea = screen.getByLabelText("Message");
    const button = screen.getByRole("button", { name: "Send" });
    await act(async () => {
      fireEvent.change(textarea, { target: { value: "hello" } });
      fireEvent.click(button);
    });
    await screen.findByText("assistant");
    expect(screen.getByText("ok")).toBeInTheDocument();
  });
});
