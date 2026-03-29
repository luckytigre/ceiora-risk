import { existsSync, readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";

const buildIdPath = ".next/BUILD_ID";
const maxAttempts = 40;
const waitMs = 100;
const settleMs = 2000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForBuildId() {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (existsSync(buildIdPath) && readFileSync(buildIdPath, "utf8").trim()) {
      // On this workspace the BUILD_ID can exist before the next generate run can reopen it.
      await sleep(settleMs);
      return;
    }
    await sleep(waitMs);
  }
  console.error(`Missing ${buildIdPath}. Run npm run build:compile first.`);
  process.exit(1);
}

await waitForBuildId();

const result = spawnSync(
  process.execPath,
  ["./node_modules/next/dist/bin/next", "build", "--experimental-build-mode", "generate"],
  {
    env: { ...process.env, NEXT_TELEMETRY_DISABLED: "1" },
    stdio: "inherit",
  },
);

if (result.error) {
  throw result.error;
}

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
