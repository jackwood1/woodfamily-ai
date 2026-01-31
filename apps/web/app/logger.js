const LEVELS = ["debug", "info", "warn", "error"];

function shouldLog(level) {
  const configured =
    (process.env.NEXT_PUBLIC_LOG_LEVEL || "info").toLowerCase();
  const currentIdx = LEVELS.indexOf(configured);
  const levelIdx = LEVELS.indexOf(level);
  if (currentIdx === -1) {
    return levelIdx >= LEVELS.indexOf("info");
  }
  return levelIdx >= currentIdx;
}

function formatMessage(level, message, extra) {
  const timestamp = new Date().toISOString();
  const base = `${timestamp} ${level.toUpperCase()} web ${message}`;
  if (extra === undefined) {
    return [base];
  }
  return [base, extra];
}

function log(level, message, extra) {
  if (!shouldLog(level)) {
    return;
  }
  const destination =
    (process.env.NEXT_PUBLIC_LOG_DESTINATION || "console").toLowerCase();
  if (destination === "console") {
    const method = console[level] || console.log;
    method(...formatMessage(level, message, extra));
  }
}

export const logger = {
  debug: (message, extra) => log("debug", message, extra),
  info: (message, extra) => log("info", message, extra),
  warn: (message, extra) => log("warn", message, extra),
  error: (message, extra) => log("error", message, extra),
};
