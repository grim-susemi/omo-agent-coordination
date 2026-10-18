import { pathToFileURL } from "node:url";
import { bundle, senpi } from "./command.mjs";

console.log(JSON.stringify({ node: process.version, bun: process.versions.bun, bundle }));
const mod = await import(pathToFileURL(bundle).href);
console.log(JSON.stringify({ exports: Object.keys(mod) }));
console.log(JSON.stringify({ senpi }));
