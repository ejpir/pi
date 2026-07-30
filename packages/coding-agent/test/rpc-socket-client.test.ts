/**
 * End-to-end tests: RpcServer on a real unix socket, driven by RpcClient.
 * No API keys required — the agent backend is a mock stream.
 */

import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { createServer, Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Agent } from "@earendil-works/pi-agent-core";
import { type AssistantMessage, type AssistantMessageEvent, EventStream, getModel } from "@earendil-works/pi-ai/compat";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentSession } from "../src/core/agent-session.ts";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import { AuthStorage } from "../src/core/auth-storage.ts";
import { SessionManager } from "../src/core/session-manager.ts";
import { SettingsManager } from "../src/core/settings-manager.ts";
import { serializeJsonLine } from "../src/modes/rpc/jsonl.ts";
import { RpcClient } from "../src/modes/rpc/rpc-client.ts";
import { createRpcSocketServer, type RpcSocketServer } from "../src/modes/rpc/rpc-socket-mode.ts";
import { RPC_PROTOCOL_VERSION } from "../src/modes/rpc/rpc-types.ts";
import { createModelRegistry, getModelRuntime } from "./model-runtime-test-utils.ts";
import { createTestResourceLoader } from "./utilities.ts";

vi.mock("../src/modes/interactive/theme/theme.js", () => ({ theme: {} }));

class MockAssistantStream extends EventStream<AssistantMessageEvent, AssistantMessage> {
	constructor() {
		super(
			(event) => event.type === "done" || event.type === "error",
			(event) => {
				if (event.type === "done") return event.message;
				if (event.type === "error") return event.error;
				throw new Error("Unexpected event type");
			},
		);
	}
}

function createAssistantMessage(text: string): AssistantMessage {
	return {
		role: "assistant",
		content: [{ type: "text", text }],
		api: "anthropic-messages",
		provider: "anthropic",
		model: "claude-sonnet-4-5",
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "stop",
		timestamp: Date.now(),
	};
}

describe("RpcClient over unix socket", () => {
	let tempDir = "";
	let socketPath = "";
	let socketServer: RpcSocketServer | undefined;
	let session: AgentSession | undefined;
	const clients: RpcClient[] = [];

	async function startFixture(options: { refreshTimeoutMs?: number } = {}): Promise<void> {
		tempDir = join(tmpdir(), `pi-rpc-sock-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		mkdirSync(tempDir, { recursive: true });
		writeFileSync(join(tempDir, "socket-test.txt"), "socket round trip\n");
		socketPath = join(tempDir, "agent.sock");

		const model = getModel("anthropic", "claude-sonnet-4-5");
		if (!model) throw new Error("Test model not found");

		const agent = new Agent({
			getApiKey: () => "test-key",
			initialState: { model, systemPrompt: "Test", tools: [] },
			streamFn: (_model, _context, _options) => {
				const stream = new MockAssistantStream();
				queueMicrotask(() => {
					stream.push({ type: "start", partial: createAssistantMessage("") });
					setTimeout(() => {
						stream.push({ type: "done", reason: "stop", message: createAssistantMessage("done") });
					}, 0);
				});
				return stream;
			},
		});

		const settingsManager = SettingsManager.create(tempDir, tempDir);
		const authStorage = AuthStorage.create(join(tempDir, "auth.json"));
		const modelRegistry = await createModelRegistry(authStorage, tempDir);

		session = new AgentSession({
			agent,
			sessionManager: SessionManager.inMemory(tempDir),
			settingsManager,
			cwd: tempDir,
			modelRuntime: getModelRuntime(modelRegistry),
			resourceLoader: createTestResourceLoader(),
		});

		const runtimeHost = {
			session,
			newSession: vi.fn(async () => ({ cancelled: true })),
			switchSession: vi.fn(async () => ({ cancelled: true })),
			fork: vi.fn(async () => ({ cancelled: true, selectedText: "" })),
			dispose: vi.fn(async () => {}),
			setRebindSession: vi.fn(),
		} as unknown as AgentSessionRuntime;

		socketServer = await createRpcSocketServer(runtimeHost, {
			socketPath,
			refreshTimeoutMs: options.refreshTimeoutMs,
		});
	}

	function newClient(): RpcClient {
		const client = new RpcClient({ socketPath });
		clients.push(client);
		return client;
	}

	afterEach(async () => {
		for (const client of clients.splice(0)) {
			try {
				await client.stop();
			} catch {
				// Already closed.
			}
		}
		await socketServer?.close();
		socketServer = undefined;
		if (session) {
			session.dispose();
			session = undefined;
		}
		if (tempDir && existsSync(tempDir)) {
			rmSync(tempDir, { recursive: true });
		}
	});

	it("reconnect() respawns a command transport after it dies", async () => {
		const { mkdtempSync, writeFileSync } = await import("node:fs");
		const { tmpdir } = await import("node:os");
		const { join } = await import("node:path");
		const dir = mkdtempSync(join(tmpdir(), "rpc-cmd-reconnect-"));
		const serverPath = join(dir, "mini-rpc.js");
		writeFileSync(
			serverPath,
			`let buf = "";
process.stdin.on("data", (d) => {
	buf += d;
	let i;
	while ((i = buf.indexOf("\\n")) >= 0) {
		const line = buf.slice(0, i);
		buf = buf.slice(i + 1);
		const o = JSON.parse(line);
		process.stdout.write(
			JSON.stringify({ id: o.id, type: "response", command: o.type, success: true, data: { output: "ok" } }) + "\\n",
		);
	}
});
process.stdout.write(
	JSON.stringify({
		type: "hello",
		protocol: 1,
		version: "test",
		sessionId: "cmd-session",
		capabilities: [],
		cwd: process.cwd(),
		resumed: false,
		state: {},
	}) + "\\n",
);
`,
		);

		const client = new RpcClient({ command: `node ${JSON.stringify(serverPath)}` });
		await client.start();
		expect(client.getHello()?.sessionId).toBe("cmd-session");

		// Kill the bridge process, then reconnect: a fresh spawn must handshake.
		(client as unknown as { process: { kill: () => void } }).process.kill();
		await new Promise((resolve) => setTimeout(resolve, 100));
		await client.reconnect();
		expect(client.getHello()?.sessionId).toBe("cmd-session");
		const result = await client.bash("true");
		expect(result.output).toBe("ok");

		await client.stop();
	});

	it("start() resolves after the hello handshake", async () => {
		await startFixture();
		const client = newClient();
		await client.start();

		const hello = client.getHello();
		expect(hello).toBeDefined();
		expect(hello!.protocol).toBe(1);
		expect(hello!.cwd).toBe(tempDir);
		expect(hello!.capabilities).toContain("fs_complete");
		expect(client.hasCapability("shutdown")).toBe(true);
	});

	it("round-trips state and filesystem commands over the socket", async () => {
		await startFixture();
		const client = newClient();
		await client.start();

		const state = await client.getState();
		expect(state.sessionId).toBe(hello_sessionId(client));
		expect(state.isStreaming).toBe(false);

		const entries = await client.fsComplete("socket-test");
		expect(entries.map((e) => e.path)).toContain("socket-test.txt");

		const file = await client.readFile("socket-test.txt");
		expect(file.content).toBe("socket round trip\n");

		function hello_sessionId(c: RpcClient): string {
			return c.getHello()!.sessionId;
		}
	});

	it("stop() detaches and the agent keeps serving the next client", async () => {
		await startFixture();
		const first = newClient();
		await first.start();
		const firstSessionId = first.getHello()!.sessionId;
		await first.stop();

		const second = newClient();
		await second.start();
		// Same agent, same session.
		expect(second.getHello()!.sessionId).toBe(firstSessionId);
	});

	it("a second concurrent client takes over; the first is notified", async () => {
		await startFixture();
		const first = newClient();
		await first.start();
		const detached = new Promise<Record<string, unknown>>((resolve) => {
			first.onEvent((event) => {
				const record = event as unknown as Record<string, unknown>;
				if (record.type === "detached") resolve(record);
			});
		});

		const second = newClient();
		await second.start();

		await expect(detached).resolves.toMatchObject({ type: "detached", reason: "takeover" });
	});

	it("a throwing event listener does not starve the other listeners", async () => {
		await startFixture();
		const first = newClient();
		await first.start();
		const saw: string[] = [];
		first.onEvent(() => {
			saw.push("thrower");
			throw new Error("boom");
		});
		first.onEvent((event) => {
			saw.push(`second:${(event as { type: string }).type}`);
		});

		const second = newClient();
		await second.start();

		await vi.waitFor(() => expect(saw).toContain("second:detached"));
		expect(saw).toContain("thrower");
	});

	it("a hung agent-side model refresh is bounded and does not jam the queue", async () => {
		await startFixture({ refreshTimeoutMs: 200 });
		// Make the agent-side catalog refresh hang forever.
		const rt = session!.modelRuntime as unknown as { refresh: () => Promise<unknown> };
		let refreshCalls = 0;
		rt.refresh = () => {
			refreshCalls++;
			return new Promise(() => {});
		};

		const client = newClient();
		await client.start();
		const started = Date.now();
		// The server answers once its bound lapses, not when the refresh settles.
		await client.refreshModels();
		expect(Date.now() - started).toBeLessThan(10_000);
		// The sequential queue is not jammed behind the still-hung refresh.
		const state = await client.getState();
		expect(state.sessionId).toBeTruthy();
		// A retry coalesces onto the in-flight refresh instead of stacking.
		await client.refreshModels();
		expect(refreshCalls).toBe(1);
	});

	it("rejects commands from a superseded (taken-over) connection", async () => {
		await startFixture();
		const client1 = newClient();
		await client1.start();
		const client2 = newClient();
		await client2.start(); // takeover

		// The old socket closes gracefully on takeover; a line racing in during
		// that window must NOT execute. (The wire-level answer is either the
		// explicit "superseded" error or a transport close — the property that
		// matters is that the mutation never lands.)
		await expect(client1.setSessionName("pwned-by-old-client")).rejects.toThrow();
		const state = await client2.getState();
		expect(state.sessionId).toBeTruthy();
		expect(state.sessionName ?? "").not.toBe("pwned-by-old-client");
	});

	it("buffers events that arrive between hello and the first listener", async () => {
		// The server re-emits pending extension dialogs immediately after
		// hello; attach-mode registers its listener only after start()
		// resolves. A dialog event landing in that window must not be dropped
		// (the agent would await its answer forever).
		const { createServer } = await import("node:net");
		const earlyDir = join(tmpdir(), `pi-rpc-early-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		mkdirSync(earlyDir, { recursive: true });
		const earlySock = join(earlyDir, "early.sock");
		const fakeServer = createServer((socket) => {
			socket.write(
				`${serializeJsonLine({ type: "hello", protocol: RPC_PROTOCOL_VERSION, version: "test", sessionId: "s", cwd: "/x", capabilities: [] })}\n`,
			);
			socket.write(
				`${serializeJsonLine({ type: "extension_ui_request", id: "dlg-1", method: "confirm", message: "proceed?" })}\n`,
			);
		});
		try {
			await new Promise<void>((resolve) => fakeServer.listen(earlySock, () => resolve()));
			const client = new RpcClient({ socketPath: earlySock });
			clients.push(client);
			await client.start();
			// Let the dialog line arrive while NO listener is registered.
			await new Promise((resolve) => setTimeout(resolve, 50));
			const seen: unknown[] = [];
			client.onEvent((event) => seen.push(event));
			expect(seen).toHaveLength(1);
			expect((seen[0] as { id?: string }).id).toBe("dlg-1");
			await client.stop().catch(() => {});
		} finally {
			(fakeServer as { closeAllConnections?: () => void }).closeAllConnections?.();
			await new Promise((resolve) => fakeServer.close(() => resolve(undefined)));
			rmSync(earlyDir, { recursive: true, force: true });
		}
	});

	it("void commands surface error responses instead of swallowing them", async () => {
		await startFixture();
		const client = newClient();
		await client.start();
		// The server rejects an empty session name; a bare-send client would
		// report success to the session picker.
		await expect(client.setSessionName("")).rejects.toThrow(/cannot be empty/i);
	});

	it("shutdown command terminates the server and closes the client", async () => {
		await startFixture();
		const client = newClient();
		await client.start();

		const closed = new Promise<void>((resolve) => {
			client.onClose(() => resolve());
		});
		await client.shutdown();
		await closed;

		// Listener is gone: a new connection is refused.
		const probe = new RpcClient({ socketPath, helloTimeoutMs: 500 });
		await expect(probe.start()).rejects.toThrow();
	});

	it("survives an abruptly reset client connection", async () => {
		await startFixture();

		// Connect, write partial garbage, then hard-destroy (simulates a client
		// crash mid-write, which surfaces server-side as ECONNRESET).
		const raw = new Socket();
		await new Promise<void>((resolve) => raw.connect(socketPath, resolve));
		raw.write('{"type":"get_stat');
		raw.destroy();

		// The server must still serve the next client.
		const client = newClient();
		await client.start();
		expect(client.getHello()!.protocol).toBe(1);
		const state = await client.getState();
		expect(state.isStreaming).toBe(false);
	});

	it("requireHello fails fast against a non-RPC socket", async () => {
		tempDir = join(tmpdir(), `pi-rpc-sock-bogus-${Date.now()}`);
		mkdirSync(tempDir, { recursive: true });
		socketPath = join(tempDir, "bogus.sock");

		// A server that accepts connections but never speaks the protocol.
		const bogus = createServer((socket) => {
			socket.write("SSH-2.0-not-an-rpc-server\r\n");
		});
		await new Promise<void>((resolve) => bogus.listen(socketPath, resolve));

		const client = new RpcClient({ socketPath, helloTimeoutMs: 300 });
		clients.push(client);
		try {
			await expect(client.start()).rejects.toThrow(/hello/i);
		} finally {
			// Stop the client first: server.close() waits for connections to end.
			await client.stop();
			await new Promise<void>((resolve) => bogus.close(() => resolve()));
		}
	});
});
