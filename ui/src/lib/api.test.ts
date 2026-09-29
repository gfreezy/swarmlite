import { expect, test } from "vitest";
import { jobDuration, type JobExecution } from "./api";
test("duration uses reported finish time and does not invent completion for lost or old runs", () => {
  const job = {
    started_at_unix_ms: 1000,
    observed: "succeeded",
  } as JobExecution;
  expect(jobDuration(job, 90000)).toBe("Unavailable");
  expect(jobDuration({ ...job, finished_at_unix_ms: 62000 }, 90000)).toBe(
    "1m 1s",
  );
  expect(jobDuration({ ...job, observed: "running" }, 90000)).toBe(
    "1m 29s elapsed",
  );
  expect(jobDuration({ ...job, observed: "lost" }, 90000)).toBe("Unavailable");
});
