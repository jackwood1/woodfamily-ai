import React from "react";
import { render, screen } from "@testing-library/react";

import HomePage from "../app/page";

describe("HomePage", () => {
  beforeEach(() => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    });
  });

  it("renders the heading", async () => {
    render(<HomePage />);
    expect(screen.getByText("Home Ops Copilot")).toBeInTheDocument();
    await screen.findByText("No reminders yet.");
  });

  it("renders reminders section", async () => {
    render(<HomePage />);
    expect(screen.getByText("Reminders")).toBeInTheDocument();
    await screen.findByText("No reminders yet.");
    expect(screen.getByText("Create Reminder")).toBeInTheDocument();
  });
});
