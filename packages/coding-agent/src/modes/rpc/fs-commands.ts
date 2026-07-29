/**
 * Filesystem helpers backing the RPC `fs_complete` and `read_file` commands.
 *
 * These run against the agent-side filesystem so that clients attached over a
 * transport (socket, ssh, container exec) can offer path completion and file
 * mentions without local filesystem access.
 *
 * v1 intentionally does not respect .gitignore (unlike the TUI's fd-based
 * completion); it prunes a fixed set of noisy directories instead.
 */

import { type Dirent, promises as fs, type Stats } from "node:fs";
import { isAbsolute, join, relative, resolve, sep } from "node:path";

/** Directories never descended into (and never returned). */
const PRUNE_DIRS = new Set([".git", ".hg", ".svn", "node_modules"]);

/** Maximum directory depth walked for completion (relative to the walk base). */
const MAX_DEPTH = 6;

/** Maximum directory entries read during a single completion walk. */
const MAX_SCANNED_ENTRIES = 20_000;

/** Maximum bytes returned by read_file before truncation. */
const MAX_READ_BYTES = 1024 * 1024;

/** Bytes inspected for binary detection. */
const BINARY_SNIFF_BYTES = 8192;

export interface FsCompletionEntry {
	/** Path relative to the walk base (the directory part of the prefix). Uses forward slashes. */
	path: string;
	isDirectory: boolean;
}

/**
 * Case-insensitive subsequence match (fuzzy). Returns true if every character
 * of `query` appears in `candidate` in order.
 */
function fuzzyMatch(query: string, candidate: string): boolean {
	if (!query) return true;
	const q = query.toLowerCase();
	const c = candidate.toLowerCase();
	let qi = 0;
	for (let ci = 0; ci < c.length && qi < q.length; ci++) {
		if (c[ci] === q[qi]) qi++;
	}
	return qi === q.length;
}

/**
 * Complete a path prefix against the filesystem rooted at `cwd`.
 *
 * Semantics mirror the TUI's fd-based completion closely enough for `@file`
 * suggestions:
 * - The directory part of `prefix` scopes the walk; the remainder is matched
 *   fuzzily against each candidate's path relative to that scope.
 * - A prefix without a "/" walks from `cwd` and matches against the full
 *   relative path.
 * - An empty prefix lists the immediate children of `cwd`.
 *
 * Returned paths are relative to `cwd` with forward slashes so clients can
 * substitute them into the user's input directly.
 */
export async function completePaths(cwd: string, prefix: string, limit = 100): Promise<FsCompletionEntry[]> {
	const root = resolve(cwd);
	const normalizedPrefix = prefix.replaceAll("\\", "/");

	let baseDir: string;
	let scopeRel: string; // directory part of the prefix, relative to root ("" for root)
	let query: string;

	const slash = normalizedPrefix.lastIndexOf("/");
	if (slash === -1) {
		baseDir = root;
		scopeRel = "";
		query = normalizedPrefix;
	} else {
		scopeRel = normalizedPrefix.slice(0, slash);
		query = normalizedPrefix.slice(slash + 1);
		const scoped = isAbsolute(scopeRel) ? resolve(scopeRel) : resolve(root, scopeRel);
		baseDir = scoped;
	}

	const results: FsCompletionEntry[] = [];
	let scanned = 0;

	async function walk(dir: string, depth: number): Promise<void> {
		if (results.length >= limit || scanned >= MAX_SCANNED_ENTRIES) return;
		let entries: Dirent[];
		try {
			entries = await fs.readdir(dir, { withFileTypes: true });
		} catch {
			return;
		}
		for (const entry of entries) {
			if (results.length >= limit || scanned >= MAX_SCANNED_ENTRIES) return;
			scanned++;
			const name = entry.name;
			if (PRUNE_DIRS.has(name)) continue;
			if (name.startsWith(".") && depth === 0 && query && !query.startsWith(".")) continue;

			const full = join(dir, name);
			const relToScope = scopeRel ? relative(baseDir, full) : relative(root, full);
			const relToRoot = relative(root, full);
			const isDir = entry.isDirectory();

			// Match the query fuzzily against the path relative to the scope,
			// falling back to the basename so "read" finds "docs/README.md".
			const candidate = relToScope.split(sep).join("/");
			if (fuzzyMatch(query, candidate) || fuzzyMatch(query, name)) {
				results.push({ path: relToRoot.split(sep).join("/"), isDirectory: isDir });
			}

			if (isDir && depth < MAX_DEPTH) {
				await walk(full, depth + 1);
			}
		}
	}

	if (query === "" && slash === -1) {
		// Empty prefix: immediate children only.
		let entries: Dirent[];
		try {
			entries = await fs.readdir(root, { withFileTypes: true });
		} catch {
			return [];
		}
		for (const entry of entries) {
			if (results.length >= limit) break;
			if (PRUNE_DIRS.has(entry.name) || entry.name.startsWith(".")) continue;
			results.push({ path: entry.name, isDirectory: entry.isDirectory() });
		}
	} else {
		await walk(baseDir, 0);
	}

	// Shallow paths first, then alphabetical — mirrors fd's practical ordering.
	results.sort((a, b) => {
		const depthA = a.path.split("/").length;
		const depthB = b.path.split("/").length;
		if (depthA !== depthB) return depthA - depthB;
		return a.path.localeCompare(b.path);
	});

	return results.slice(0, limit);
}

export interface ReadFileResult {
	path: string;
	content: string;
	truncated: boolean;
}

/**
 * Read a UTF-8 text file for file mentions. Relative paths resolve against
 * `cwd`. No confinement is applied: the agent-side filesystem *is* the
 * security boundary (confine via the sandbox/runtime, not here).
 */
export async function readSessionFile(cwd: string, filePath: string): Promise<ReadFileResult> {
	const resolved = isAbsolute(filePath) ? resolve(filePath) : resolve(cwd, filePath);

	let stat: Stats;
	try {
		stat = await fs.stat(resolved);
	} catch {
		throw new Error(`File not found: ${filePath}`);
	}
	if (!stat.isFile()) {
		throw new Error(`Not a file: ${filePath}`);
	}

	const handle = await fs.open(resolved, "r");
	try {
		const size = stat.size;
		const toRead = Math.min(size, MAX_READ_BYTES);
		const buffer = Buffer.alloc(toRead);
		await handle.read(buffer, 0, toRead, 0);

		// Binary sniff: NUL byte in the inspected region.
		const sniff = buffer.subarray(0, Math.min(toRead, BINARY_SNIFF_BYTES));
		if (sniff.includes(0)) {
			throw new Error(`Binary file: ${filePath}`);
		}

		return {
			path: resolved,
			content: buffer.toString("utf8"),
			truncated: size > MAX_READ_BYTES,
		};
	} finally {
		await handle.close();
	}
}

/** Re-exported for tests. */
export const FS_COMMAND_LIMITS = {
	MAX_DEPTH,
	MAX_SCANNED_ENTRIES,
	MAX_READ_BYTES,
};
