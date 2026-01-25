import React from "react";
import { render, screen } from "@testing-library/react";

import HomePage from "../app/page";

describe("HomePage", () => {
  it("renders the heading", () => {
    render(<HomePage />);
    expect(screen.getByText("Home Ops Copilot")).toBeInTheDocument();
  });
});
