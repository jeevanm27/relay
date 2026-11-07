#!/usr/bin/env node
// Builds the two Docker images (relay/relay-server:dev and relay/relay-worker:dev) from the jars in target/.
// Run `mvn -B -DskipTests package` first.  Usage:  node bench/build-images.mjs
// It uses a small staging folder in the OS temp directory, so it also works when the project lives in a folder
// that Docker cannot read directly (for example OneDrive "files on demand").
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildImages } from "./lib.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
buildImages(root)
  .then(() => console.log("built relay/relay-server:dev and relay/relay-worker:dev"))
  .catch((e) => { console.error(e.message); process.exit(1); });
