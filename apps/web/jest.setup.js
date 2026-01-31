import "@testing-library/jest-dom";

if (!global.fetch) {
  global.fetch = jest.fn().mockResolvedValue({
    ok: true,
    json: async () => [],
  });
}
