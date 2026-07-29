import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { completePaths, readSessionFile } from "../src/modes/rpc/fs-commands.ts";

describe("rpc fs-commands", () => {
	let root: string;

	beforeEach(() => {
		root = join(tmpdir(), `pi-rpc-fs-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		mkdirSync(join(root, "src", "components"), { recursive: true });
		mkdirSync(join(root, "docs"), { recursive: true });
		mkdirSync(join(root, ".git", "objects"), { recursive: true });
		mkdirSync(join(root, "node_modules", "dep"), { recursive: true });
		writeFileSync(join(root, "README.md"), "# hello\n");
		writeFileSync(join(root, "src", "index.ts"), "export {};\n");
		writeFileSync(join(root, "src", "components", "button.tsx"), "export {};\n");
		writeFileSync(join(root, "docs", "guide.md"), "guide\n");
		writeFileSync(join(root, ".git", "objects", "abc"), "binary");
		writeFileSync(join(root, "node_modules", "dep", "index.js"), "module.exports = {};\n");
		writeFileSync(join(root, ".hidden"), "secret\n");
	});

	afterEach(() => {
		if (root && existsSync(root)) {
			rmSync(root, { recursive: true });
		}
	});

	describe("completePaths", () => {
		it("lists immediate children for an empty prefix", async () => {
			const entries = await completePaths(root, "");
			const paths = entries.map((e) => e.path);
			expect(paths).toContain("README.md");
			expect(paths).toContain("src");
			expect(paths).toContain("docs");
			// Dotfiles and pruned dirs are hidden
			expect(paths).not.toContain(".git");
			expect(paths).not.toContain(".hidden");
			expect(paths).not.toContain("node_modules");
			expect(entries.find((e) => e.path === "src")?.isDirectory).toBe(true);
			expect(entries.find((e) => e.path === "README.md")?.isDirectory).toBe(false);
		});

		it("matches fuzzily across the tree", async () => {
			const entries = await completePaths(root, "button");
			expect(entries.map((e) => e.path)).toContain("src/components/button.tsx");
		});

		it("scopes the walk to the directory part of the prefix", async () => {
			const entries = await completePaths(root, "src/comp");
			expect(entries.map((e) => e.path)).toContain("src/components");
			expect(entries.map((e) => e.path)).not.toContain("README.md");
		});

		it("never descends into .git or node_modules", async () => {
			const entries = await completePaths(root, "index");
			const paths = entries.map((e) => e.path);
			expect(paths).toContain("src/index.ts");
			expect(paths.some((p) => p.startsWith("node_modules"))).toBe(false);
			expect(paths.some((p) => p.startsWith(".git"))).toBe(false);
		});

		it("respects the result limit", async () => {
			for (let i = 0; i < 20; i++) {
				writeFileSync(join(root, "docs", `file-${String(i).padStart(2, "0")}.md`), "x\n");
			}
			const entries = await completePaths(root, "docs/file", 5);
			expect(entries.length).toBeLessThanOrEqual(5);
		});
	});

	describe("readSessionFile", () => {
		it("reads a file relative to cwd", async () => {
			const result = await readSessionFile(root, "README.md");
			expect(result.content).toBe("# hello\n");
			expect(result.truncated).toBe(false);
		});

		it("reads absolute paths", async () => {
			const result = await readSessionFile(root, join(root, "docs", "guide.md"));
			expect(result.content).toBe("guide\n");
		});

		it("rejects missing files", async () => {
			await expect(readSessionFile(root, "nope.txt")).rejects.toThrow(/not found/i);
		});

		it("rejects directories", async () => {
			await expect(readSessionFile(root, "src")).rejects.toThrow(/not a file/i);
		});

		it("rejects binary files", async () => {
			writeFileSync(join(root, "bin.dat"), Buffer.from([0x89, 0x50, 0x00, 0x4e]));
			await expect(readSessionFile(root, "bin.dat")).rejects.toThrow(/binary/i);
		});

		it("truncates large files", async () => {
			const big = join(root, "big.txt");
			writeFileSync(big, Buffer.alloc(1024 * 1024 + 100, "a"));
			const result = await readSessionFile(root, "big.txt");
			expect(result.truncated).toBe(true);
			expect(result.content.length).toBe(1024 * 1024);
		});
	});
});
