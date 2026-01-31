const LEVELS = ["debug", "info", "warn", "error"] as const;
type LogLevel = (typeof LEVELS)[number];

function shouldLog(level: LogLevel) {
  const configured =
    (process.env.NEXT_PUBLIC_LOG_LEVEL || "info").toLowerCase();
  const currentIdx = LEVELS.indexOf(configured as LogLevel);
  const levelIdx = LEVELS.indexOf(level);
  if (currentIdx === -1) {
    return levelIdx >= LEVELS.indexOf("info");
  }
  return levelIdx >= currentIdx;
}

function formatMessage(level: LogLevel, message: string, extra?: unknown) {
  const timestamp = new Date().toISOString();
  const base = `${timestamp} ${level.toUpperCase()} web ${message}`;
  if (extra === undefined) {
    return [base];
  }
  return [base, extra];
}

function log(level: LogLevel, message: string, extra?: unknown) {
  if (!shouldLog(level)) {
    return;
  }
  const destination =
    (process.env.NEXT_PUBLIC_LOG_DESTINATION || "console").toLowerCase();
  if (destination === "console") {
    const consoleMap = console as Record<string, (...args: unknown[]) => void>;
    const method = consoleMap[level] || console.log;
    method(...formatMessage(level, message, extra));
  }
}

export const logger = {
  debug: (message: string, extra?: unknown) => log("debug", message, extra),
  info: (message: string, extra?: unknown) => log("info", message, extra),
  warn: (message: string, extra?: unknown) => log("warn", message, extra),
  error: (message: string, extra?: unknown) => log("error", message, extra),
};
