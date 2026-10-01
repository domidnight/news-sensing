import { cp, mkdir, rm } from "node:fs/promises";
import { build } from "esbuild";

const outdir = "dist-pages";

await rm(outdir, { recursive: true, force: true });
await mkdir(outdir, { recursive: true });
await cp("public", outdir, { recursive: true });

await build({
  entryPoints: ["src/index.js"],
  outfile: `${outdir}/_worker.js`,
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  minify: false,
});

console.log(`Cloudflare Pages build ready in ${outdir}`);
