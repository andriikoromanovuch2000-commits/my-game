const fs = require("node:fs");
const path = require("node:path");

const projectRoot = path.join(__dirname, "..");
const outputDirectory = path.join(projectRoot, "dist");
const frontendFiles = ["index.html", "backend-config.js", "404.html"];

fs.rmSync(outputDirectory, { recursive: true, force: true });
fs.mkdirSync(outputDirectory, { recursive: true });

for (const file of frontendFiles) {
  fs.copyFileSync(path.join(projectRoot, file), path.join(outputDirectory, file));
}
