import React from "react";
import "./globals.css";

export const metadata = {
  title: "Home Ops Copilot",
  description: "Admin console for Home Ops Copilot",
  icons: {
    icon: "/favicon.svg",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
