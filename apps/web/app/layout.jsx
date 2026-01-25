import React from "react";
import "./globals.css";

export const metadata = {
  title: "Home Ops Copilot",
  description: "Admin console for Home Ops Copilot",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
