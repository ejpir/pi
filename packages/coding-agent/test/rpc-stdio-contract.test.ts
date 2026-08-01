/**
 * Wire contract of the stdio RPC entry point, exercised the way an external
 * supervisor does (e.g. @earendil-works/server's rpc-process): spawn the
 * built rpc-entry, read raw JSONL from stdout, write raw JSONL commands.
 * Deliberately does NOT use RpcClient — the contract under test is the byte
 * stream itself. Needs no credentials: no command here reaches a provider.
 */

import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

const RPC_ENTRY = join(__dirname, "..", "dist", "rpc-entry.js");

interface LineReader {
	next(timeoutMs?: number): Promise<Record<string, unknown>>;
}

function readLines(child: ChildProcess): LineReader {
	const queue: Record<string, unknown>[] = [];
	const waiters: Array<(line: Record<string, unknown>) => void> = [];
	let buffer = "";
	child.stdout?.setEncoding("utf8");
	child.stdout?.on("data", (chunk: string) => {
		buffer += chunk;
		let index = buffer.indexOf("\n");
		while (index !== -1) {
			const line = buffer.slice(0, index).trim();
			buffer = buffer.slice(index + 1);
			if (line) {
				const parsed = JSON.parse(line) as Record<string, unknown>;
				const waiter = waiters.shift();
				if (waiter) waiter(parsed);
				else queue.push(parsed);
			}
			index = buffer.indexOf("\n");
		}
	});
	return {
		next(timeoutMs = 15_000) {
			const queued = queue.shift();
			if (queued) return Promise.resolve(queued);
			return new Promise((resolve, reject) => {
				const timer = setTimeout(() => reject(new Error("Timed out waiting for a JSONL line")), timeoutMs);
				waiters.push((line) => {
					clearTimeout(timer);
					resolve(line);
				});
			});
		},
	};
}

async function nextResponse(reader: LineReader, id: string): Promise<Record<string, unknown>> {
	// Skip interleaved events; the supervisor does the same (routes by type/id).
	for (;;) {
		const line = await reader.next();
		if (line.type === "response" && line.id === id) return line;
	}
}

describe.skipIf(!existsSync(RPC_ENTRY))("stdio RPC wire contract (supervisor path)", () => {
	let child: ChildProcess | undefined;
	let tempDir: string | undefined;

	afterEach(() => {
		if (child && child.exitCode === null) child.kill("SIGKILL");
		child = undefined;
		if (tempDir && existsSync(tempDir)) rmSync(tempDir, { recursive: true });
		tempDir = undefined;
	});

	it("greets with hello, answers commands, errors on unknown types, exits on shutdown", async () => {
		tempDir = join(tmpdir(), `pi-stdio-contract-${Date.now()}`);
		mkdirSync(tempDir, { recursive: true });
		child = spawn("node", [RPC_ENTRY], {
			cwd: tempDir,
			env: { ...process.env, PI_CODING_AGENT_DIR: tempDir },
			stdio: ["pipe", "pipe", "pipe"],
		});
		let stderr = "";
		child.stderr?.setEncoding("utf8");
		child.stderr?.on("data", (chunk: string) => {
			stderr += chunk;
		});
		const exited = new Promise<number | null>((resolve) => child?.once("exit", (code) => resolve(code)));
		const reader = readLines(child);

		// The FIRST line is the hello greeting (new vs upstream's silent
		// start; external consumers routing by type forward it as an event).
		let hello: Record<string, unknown>;
		try {
			hello = await reader.next();
		} catch (error) {
			throw new Error(`${error instanceof Error ? error.message : String(error)}. Stderr: ${stderr}`);
		}
		expect(hello.type).toBe("hello");
		expect(hello.protocol).toBe(1);
		expect(hello.capabilities).toContain("shutdown");
		expect(typeof hello.sessionId).toBe("string");

		child.stdin?.write(`${JSON.stringify({ type: "get_state", id: "s1" })}\n`);
		const state = await nextResponse(reader, "s1");
		expect(state.success).toBe(true);

		child.stdin?.write(`${JSON.stringify({ type: "get_commands", id: "c1" })}\n`);
		const commands = await nextResponse(reader, "c1");
		expect(commands.success).toBe(true);

		// Unknown command types must produce an error response, never a crash
		// or a swallowed line — the supervisor correlates strictly by id.
		child.stdin?.write(`${JSON.stringify({ type: "not_a_real_command", id: "u1" })}\n`);
		const unknown = await nextResponse(reader, "u1");
		expect(unknown.success).toBe(false);

		child.stdin?.write(`${JSON.stringify({ type: "shutdown", id: "sd1" })}\n`);
		const code = await Promise.race([
			exited,
			new Promise<never>((_, reject) =>
				setTimeout(() => reject(new Error(`shutdown did not exit. Stderr: ${stderr}`)), 15_000),
			),
		]);
		expect(code).toBe(0);
	}, 60_000);
});
