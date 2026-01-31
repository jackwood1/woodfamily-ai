import { logger } from "../app/logger";

describe("logger", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
    jest.spyOn(console, "info").mockImplementation(() => {});
  });

  afterEach(() => {
    console.info.mockRestore();
    process.env = originalEnv;
  });

  it("respects log level", () => {
    process.env.NEXT_PUBLIC_LOG_LEVEL = "error";
    logger.info("should_not_log");
    expect(console.info).not.toHaveBeenCalled();
  });
});
