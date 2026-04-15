/* eslint-disable @typescript-eslint/no-require-imports */
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const appRoot = path.resolve(__dirname, "..");
const standaloneRoot = path.join(appRoot, ".next", "standalone");
const standaloneServer = path.join(standaloneRoot, "server.js");
const staticSource = path.join(appRoot, ".next", "static");
const staticDestination = path.join(standaloneRoot, ".next", "static");
const publicSource = path.join(appRoot, "public");
const publicDestination = path.join(standaloneRoot, "public");

if (!fs.existsSync(standaloneServer)) {
  console.error("Standalone build not found. Run `npm run build` first.");
  process.exit(1);
}

const syncDirectory = (source, destination) => {
  if (!fs.existsSync(source)) return;
  fs.rmSync(destination, { recursive: true, force: true });
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.cpSync(source, destination, { recursive: true, force: true });
};

syncDirectory(staticSource, staticDestination);
syncDirectory(publicSource, publicDestination);

const child = spawn(process.execPath, [standaloneServer], {
  cwd: standaloneRoot,
  stdio: "inherit",
  env: {
    ...process.env,
    HOSTNAME: process.env.HOSTNAME || "0.0.0.0",
  },
});

child.on("error", (error) => {
  console.error("Failed to start standalone server:", error);
  process.exit(1);
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});
