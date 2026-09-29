import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";
export default defineConfig({
  plugins: [react()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  test: {
    environment: "jsdom",
    setupFiles: ["src/test-setup.ts"],
    environmentOptions: { jsdom: { url: "http://127.0.0.1:17081/" } },
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
