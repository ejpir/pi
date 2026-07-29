import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Agent } from "@earendil-works/pi-agent-core";
import { type AssistantMessage, type AssistantMessageEvent, EventStream, getModel } from "@earendil-works/pi-ai/compat";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { AgentSession } from "../src/core/agent-session.ts";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import { AuthStorage } from "../src/core/auth-storage.ts";
import type { ExtensionFactory } from "../src/core/extensions/types.ts";
import type { PromptTemplate } from "../src/core/prompt-templates.ts";
import { SessionManager } from "../src/core/session-manager.ts";
import { SettingsManager } from "../src/core/settings-manager.ts";
import { runRpcMode } from "../src/modes/rpc/rpc-mode.ts";
import { type RpcConnection, RpcServer } from "../src/modes/rpc/rpc-server.ts";
import { RPC_CAPABILITIES } from "../src/modes/rpc/rpc-types.ts";
import { createModelRegistry, getModelRuntime } from "./model-runtime-test-utils.ts";
import { createTestExtensionsResult, createTestResourceLoader } from "./utilities.ts";

// Mock stdio plumbing so runRpcMode can be driven in-process (same pattern as
// rpc-prompt-response-semantics.test.ts).
const rpcIo = vi.hoisted(() => ({
	outputLines: [] as string[],
	lineHandler: undefined as ((line: string) => void) | undefined,
}));

vi.mock("../src/core/output-guard.js", () => ({
	flushRawStdout: vi.fn(async () => {}),
	takeOverStdout: vi.fn(),
	waitForRawStdoutBackpressure: vi.fn(async () => {}),
	writeRawStdout: (line: string) => {
		rpcIo.outputLines.push(line);
	},
}));

vi.mock("../src/modes/interactive/theme/theme.js", () => ({ theme: {} }));

vi.mock("../src/modes/rpc/jsonl.js", () => ({
	attachJsonlLineReader: vi.fn((_stream: NodeJS.ReadableStream, onLine: (line: string) => void) => {
		rpcIo.lineHandler = onLine;
		return () => {};
	}),
	serializeJsonLine: (value: unknown) => `${JSON.stringify(value)}\n`,
}));

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

function parseOutput(outputLines: string[]): Array<Record<string, unknown>> {
	return outputLines
		.flatMap((line) => line.split("\n"))
		.filter((line) => line.trim().length > 0)
		.map((line) => JSON.parse(line) as Record<string, unknown>);
}

interface HarnessOptions {
	extensions?: ExtensionFactory[];
	sessionDir?: boolean;
	prompts?: Array<{ name: string; description: string; argumentHint?: string; content: string }>;
}

async function createRuntimeHost(
	tempDir: string,
	options: HarnessOptions = {},
): Promise<{
	runtimeHost: AgentSessionRuntime;
	session: AgentSession;
	cleanup: () => Promise<void>;
}> {
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

	const sessionManager = options.sessionDir
		? SessionManager.create(tempDir, join(tempDir, "sessions"))
		: SessionManager.inMemory(tempDir);
	const settingsManager = SettingsManager.create(tempDir, tempDir);
	const authStorage = AuthStorage.inMemory({
		anthropic: { type: "api_key", key: "test-key" },
	});
	const modelRegistry = await createModelRegistry(authStorage, tempDir);

	const extensionsResult = options.extensions
		? await createTestExtensionsResult(options.extensions, tempDir)
		: undefined;

	const prompts: PromptTemplate[] | undefined = options.prompts?.map((p) => ({
		...p,
		filePath: join(tempDir, `${p.name}.md`),
		sourceInfo: { source: "user", path: join(tempDir, `${p.name}.md`), scope: "user", origin: "top-level" },
	}));

	const session = new AgentSession({
		agent,
		sessionManager,
		settingsManager,
		cwd: tempDir,
		modelRuntime: getModelRuntime(modelRegistry),
		resourceLoader: createTestResourceLoader({ extensionsResult, prompts }),
	});

	const runtimeHost = {
		session,
		newSession: vi.fn(async () => ({ cancelled: true })),
		switchSession: vi.fn(async () => ({ cancelled: true })),
		fork: vi.fn(async () => ({ cancelled: true, selectedText: "" })),
		dispose: vi.fn(async () => {}),
		setRebindSession: vi.fn(),
	} as unknown as AgentSessionRuntime;

	return {
		runtimeHost,
		session,
		cleanup: async () => {
			try {
				if (session.isStreaming) await session.abort();
			} catch {
				// ignore
			}
			session.dispose();
			if (existsSync(tempDir)) rmSync(tempDir, { recursive: true });
		},
	};
}

function sendCommand(command: Record<string, unknown>): void {
	rpcIo.lineHandler?.(JSON.stringify(command));
}

async function waitForOutput(predicate: (records: Array<Record<string, unknown>>) => boolean): Promise<void> {
	await vi.waitFor(() => {
		expect(predicate(parseOutput(rpcIo.outputLines))).toBe(true);
	});
}

describe("stdio RPC mode lifecycle", () => {
	let tempDir = "";
	let cleanup = async () => {};

	beforeAll(async () => {
		tempDir = join(tmpdir(), `pi-rpc-lifecycle-${Date.now()}`);
		mkdirSync(join(tempDir, "src"), { recursive: true });
		writeFileSync(join(tempDir, "README.md"), "hello readme\n");
		writeFileSync(join(tempDir, "src", "main.ts"), "export {};\n");

		// Fixture session file for list_sessions (sessions are only flushed to
		// disk on first append, which never happens in this harness).
		const sessionsDir = join(tempDir, "sessions");
		mkdirSync(sessionsDir, { recursive: true });
		writeFileSync(
			join(sessionsDir, "2026-01-01T00-00-00-000_fixture-session.jsonl"),
			`${JSON.stringify({ type: "session", id: "fixture-session", timestamp: new Date().toISOString(), cwd: tempDir })}\n`,
		);

		rpcIo.outputLines = [];
		rpcIo.lineHandler = undefined;
		const host = await createRuntimeHost(tempDir, { sessionDir: true });
		cleanup = host.cleanup;
		void runRpcMode(host.runtimeHost);
		await vi.waitFor(() => expect(rpcIo.lineHandler).toBeDefined());
	});

	afterAll(async () => {
		await cleanup();
	});

	it("emits hello as the first protocol line", () => {
		const records = parseOutput(rpcIo.outputLines);
		expect(records.length).toBeGreaterThan(0);
		const hello = records[0];
		expect(hello.type).toBe("hello");
		expect(hello.protocol).toBe(1);
		expect(typeof hello.version).toBe("string");
		expect(typeof hello.sessionId).toBe("string");
		expect(hello.cwd).toBe(tempDir);
		for (const capability of RPC_CAPABILITIES) {
			expect(hello.capabilities as string[]).toContain(capability);
		}
	});

	it("answers fs_complete against the session cwd", async () => {
		sendCommand({ id: "c1", type: "fs_complete", prefix: "READ" });
		await waitForOutput((records) =>
			records.some((r) => r.id === "c1" && r.type === "response" && r.command === "fs_complete"),
		);
		const response = parseOutput(rpcIo.outputLines).find((r) => r.id === "c1")!;
		expect(response.success).toBe(true);
		const entries = (response.data as { entries: Array<{ path: string; isDirectory: boolean }> }).entries;
		expect(entries.map((e) => e.path)).toContain("README.md");
	});

	it("answers read_file with content", async () => {
		sendCommand({ id: "c2", type: "read_file", path: "README.md" });
		await waitForOutput((records) => records.some((r) => r.id === "c2" && r.type === "response"));
		const response = parseOutput(rpcIo.outputLines).find((r) => r.id === "c2")!;
		expect(response.success).toBe(true);
		expect((response.data as { content: string }).content).toBe("hello readme\n");
	});

	it("answers read_file errors for missing files", async () => {
		sendCommand({ id: "c3", type: "read_file", path: "nope.txt" });
		await waitForOutput((records) => records.some((r) => r.id === "c3" && r.type === "response"));
		const response = parseOutput(rpcIo.outputLines).find((r) => r.id === "c3")!;
		expect(response.success).toBe(false);
		expect(String(response.error)).toMatch(/not found/i);
	});

	it("answers list_sessions with the current session", async () => {
		sendCommand({ id: "c4", type: "list_sessions" });
		await waitForOutput((records) => records.some((r) => r.id === "c4" && r.type === "response"));
		const response = parseOutput(rpcIo.outputLines).find((r) => r.id === "c4")!;
		expect(response.success).toBe(true);
		const sessions = (response.data as { sessions: Array<{ id: string; created: string; cwd: string }> }).sessions;
		expect(sessions.length).toBe(1);
		expect(sessions[0].id).toBe("fixture-session");
		expect(typeof sessions[0].created).toBe("string");
		expect(sessions[0].cwd).toBe(tempDir);
	});
});

// ============================================================================
// Attachment semantics (RpcServer + fake connections)
// ============================================================================

class FakeConnection implements RpcConnection {
	sent: Array<Record<string, unknown>> = [];
	closed = false;
	private lineCb: ((line: string) => void) | undefined;
	private closeCb: (() => void) | undefined;

	send(obj: unknown): void {
		this.sent.push(obj as Record<string, unknown>);
	}
	onLine(cb: (line: string) => void): void {
		this.lineCb = cb;
	}
	onClose(cb: () => void): void {
		this.closeCb = cb;
	}
	close(): void {
		if (this.closed) return;
		this.closed = true;
		this.closeCb?.();
	}
	async flush(): Promise<void> {}

	/** Simulate a transport drop without close(). */
	drop(): void {
		this.closed = true;
		this.closeCb?.();
	}
	emit(command: Record<string, unknown>): void {
		this.lineCb?.(JSON.stringify(command));
	}

	lastHello(): Record<string, unknown> | undefined {
		return this.sent.find((o) => o.type === "hello");
	}
	uiRequests(): Array<Record<string, unknown>> {
		return this.sent.filter((o) => o.type === "extension_ui_request");
	}
}

describe("RpcServer attachment semantics", () => {
	let tempDir: string;
	const cleanups: Array<() => Promise<void>> = [];

	afterEach(async () => {
		const toRun = cleanups.splice(0);
		for (const cleanup of toRun) await cleanup();
		if (tempDir && existsSync(tempDir)) rmSync(tempDir, { recursive: true });
	});

	async function startServer(options: {
		graceMs?: number;
		confirmResults?: boolean[];
		onShutdown?: (exitCode: number) => void;
	}): Promise<{ server: RpcServer; confirmResults: boolean[] }> {
		tempDir = join(tmpdir(), `pi-rpc-server-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		mkdirSync(tempDir, { recursive: true });

		const confirmResults: boolean[] = options.confirmResults ?? [];
		const uiProbe: ExtensionFactory = (pi) => {
			pi.on("user_bash", async (_event, ctx) => {
				const ok = await ctx.ui.confirm("Run command?", "bash wants to run");
				confirmResults.push(ok);
				return undefined;
			});
		};

		const host = await createRuntimeHost(tempDir, { extensions: [uiProbe] });
		cleanups.push(host.cleanup);

		const server = new RpcServer(host.runtimeHost, {
			connectionLoss: "grace",
			detachGraceMs: options.graceMs ?? 30_000,
			onShutdown: options.onShutdown ?? (() => {}),
		});
		await server.start();
		return { server, confirmResults };
	}

	it("greets each attached client with hello", async () => {
		const { server } = await startServer({});
		const conn = new FakeConnection();
		server.attachConnection(conn);
		const hello = conn.lastHello();
		expect(hello).toBeDefined();
		expect(hello!.protocol).toBe(1);
	});

	it("a second attach takes over: the first client is notified and closed", async () => {
		const { server } = await startServer({});
		const first = new FakeConnection();
		server.attachConnection(first);

		const second = new FakeConnection();
		server.attachConnection(second);

		expect(first.sent.some((o) => o.type === "detached" && o.reason === "takeover")).toBe(true);
		expect(first.closed).toBe(true);
		expect(second.lastHello()).toBeDefined();
	});

	it("detach auto-resolves pending UI requests (headless semantics)", async () => {
		const { server, confirmResults } = await startServer({});
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "b1", type: "bash", command: "true" });
		await vi.waitFor(() => expect(conn.uiRequests().length).toBe(1));

		conn.emit({ id: "d1", type: "detach" });
		await vi.waitFor(() => expect(confirmResults).toEqual([false]));
		expect(conn.closed).toBe(true);
		expect(conn.sent.some((o) => o.id === "d1" && o.command === "detach" && o.success === true)).toBe(true);
	});

	it("holds UI requests across connection loss and re-emits them on reconnect", async () => {
		const { server, confirmResults } = await startServer({ graceMs: 5_000 });
		const first = new FakeConnection();
		server.attachConnection(first);

		first.emit({ id: "b1", type: "bash", command: "true" });
		await vi.waitFor(() => expect(first.uiRequests().length).toBe(1));
		const requestId = first.uiRequests()[0].id as string;

		// Transport drops without detach: request must NOT auto-resolve.
		first.drop();
		await new Promise((resolve) => setTimeout(resolve, 100));
		expect(confirmResults).toEqual([]);

		// Reconnect: the same request is re-emitted and can be answered.
		const second = new FakeConnection();
		server.attachConnection(second);
		await vi.waitFor(() => expect(second.uiRequests().length).toBe(1));
		expect(second.uiRequests()[0].id).toBe(requestId);

		second.emit({ type: "extension_ui_response", id: requestId, confirmed: true });
		await vi.waitFor(() => expect(confirmResults).toEqual([true]));
	});

	it("auto-resolves held requests when the grace window expires", async () => {
		const { server, confirmResults } = await startServer({ graceMs: 80 });
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "b1", type: "bash", command: "true" });
		await vi.waitFor(() => expect(conn.uiRequests().length).toBe(1));

		conn.drop();
		await vi.waitFor(() => expect(confirmResults).toEqual([false]), { timeout: 3000 });
	});

	it("shutdown command notifies the client and invokes onShutdown", async () => {
		const shutdownCalls: number[] = [];
		const { server } = await startServer({ onShutdown: (code) => shutdownCalls.push(code) });
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "s1", type: "shutdown" });
		await vi.waitFor(() => expect(shutdownCalls).toEqual([0]));
		expect(conn.sent.some((o) => o.id === "s1" && o.command === "shutdown" && o.success === true)).toBe(true);
		expect(conn.sent.some((o) => o.type === "detached" && o.reason === "shutdown")).toBe(true);
	});
});

// ============================================================================
// P2 remote-attach protocol additions
// ============================================================================

describe("RpcServer P2 attach commands", () => {
	let tempDir: string;
	const cleanups: Array<() => Promise<void>> = [];

	afterEach(async () => {
		const toRun = cleanups.splice(0);
		for (const cleanup of toRun) await cleanup();
		if (tempDir && existsSync(tempDir)) rmSync(tempDir, { recursive: true });
	});

	async function startP2Server(
		options: { prompts?: Array<{ name: string; description: string; argumentHint?: string; content: string }> } = {},
	): Promise<RpcServer> {
		tempDir = join(tmpdir(), `pi-rpc-p2-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		mkdirSync(tempDir, { recursive: true });

		const host = await createRuntimeHost(tempDir, { prompts: options.prompts });
		cleanups.push(host.cleanup);

		const server = new RpcServer(host.runtimeHost, {
			connectionLoss: "grace",
			detachGraceMs: 30_000,
			onShutdown: () => {},
		});
		await server.start();
		return server;
	}

	async function responseFor(conn: FakeConnection, id: string): Promise<Record<string, unknown>> {
		await vi.waitFor(() => expect(conn.sent.some((o) => o.id === id && o.type === "response")).toBe(true));
		return conn.sent.find((o) => o.id === id && o.type === "response")!;
	}

	it("includes scopedModels in get_state and applies set_scoped_models", async () => {
		const server = await startP2Server();
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "st1", type: "get_state" });
		const initial = await responseFor(conn, "st1");
		expect(initial.success).toBe(true);
		expect((initial.data as { scopedModels: unknown[] }).scopedModels).toEqual([]);

		conn.emit({ id: "sc1", type: "set_scoped_models", models: [{ provider: "bogus", id: "nope" }] });
		const bogus = await responseFor(conn, "sc1");
		expect(bogus.success).toBe(false);
		expect(String(bogus.error)).toMatch(/Unknown model/);

		conn.emit({ id: "am1", type: "get_available_models" });
		const available = await responseFor(conn, "am1");
		const models = (available.data as { models: Array<{ provider: string; id: string }> }).models;
		if (models.length > 0) {
			conn.emit({
				id: "sc2",
				type: "set_scoped_models",
				models: [{ provider: models[0].provider, id: models[0].id, thinkingLevel: "low" }],
			});
			const set = await responseFor(conn, "sc2");
			expect(set.success).toBe(true);

			conn.emit({ id: "st2", type: "get_state" });
			const after = await responseFor(conn, "st2");
			const scoped = (after.data as { scopedModels: Array<{ model: { id: string }; thinkingLevel?: string }> })
				.scopedModels;
			expect(scoped.length).toBe(1);
			expect(scoped[0].model.id).toBe(models[0].id);
			expect(scoped[0].thinkingLevel).toBe("low");
		}
	});

	it("answers get_context_usage / get_system_prompt / get_tools / get_resources", async () => {
		const server = await startP2Server();
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "cu1", type: "get_context_usage" });
		const usage = await responseFor(conn, "cu1");
		expect(usage.success).toBe(true);
		expect(usage.data === null || typeof usage.data === "object").toBe(true);

		conn.emit({ id: "sp1", type: "get_system_prompt" });
		const prompt = await responseFor(conn, "sp1");
		expect(prompt.success).toBe(true);
		const systemPrompt = (prompt.data as { systemPrompt: string }).systemPrompt;
		expect(systemPrompt.length).toBeGreaterThan(0);
		expect(systemPrompt).toContain(tempDir);

		conn.emit({ id: "tl1", type: "get_tools" });
		const tools = await responseFor(conn, "tl1");
		expect(tools.success).toBe(true);
		expect(Array.isArray((tools.data as { tools: unknown[] }).tools)).toBe(true);

		conn.emit({ id: "rs1", type: "get_resources" });
		const resources = await responseFor(conn, "rs1");
		expect(resources.success).toBe(true);
		const data = resources.data as {
			skills: unknown[];
			prompts: unknown[];
			themes: unknown[];
			extensions: unknown[];
			extensionErrors: unknown[];
			agentsFiles: unknown[];
			appendSystemPromptSources: unknown[];
		};
		expect(data.skills).toEqual([]);
		expect(data.prompts).toEqual([]);
		expect(data.themes).toEqual([]);
		expect(Array.isArray(data.extensions)).toBe(true);
		expect(Array.isArray(data.extensionErrors)).toBe(true);
		expect(data.agentsFiles).toEqual([]);
		expect(data.appendSystemPromptSources).toEqual([]);
	});

	it("serves prompt templates via get_resources and get_commands with argumentHint", async () => {
		const server = await startP2Server({
			prompts: [
				{
					name: "review",
					description: "Review code",
					argumentHint: "<file> [focus]",
					content: "Review this: $1",
				},
			],
		});
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "rs2", type: "get_resources" });
		const resources = await responseFor(conn, "rs2");
		expect(resources.success).toBe(true);
		const prompts = (resources.data as { prompts: Array<{ name: string; argumentHint?: string; content?: string }> })
			.prompts;
		expect(prompts.length).toBe(1);
		expect(prompts[0].name).toBe("review");
		expect(prompts[0].argumentHint).toBe("<file> [focus]");
		expect(prompts[0].content).toBeUndefined();

		conn.emit({ id: "gc1", type: "get_commands" });
		const commands = await responseFor(conn, "gc1");
		const review = (commands.data as { commands: Array<{ name: string; argumentHint?: string }> }).commands.find(
			(c) => c.name === "review",
		);
		expect(review).toBeDefined();
		expect(review!.argumentHint).toBe("<file> [focus]");
	});

	it("answers clear_queue / abort_compaction / abort_branch_summary / get_auth_status", async () => {
		const server = await startP2Server();
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "cq1", type: "clear_queue" });
		const cleared = await responseFor(conn, "cq1");
		expect(cleared.success).toBe(true);
		expect(cleared.data).toEqual({ steering: [], followUp: [] });

		conn.emit({ id: "ac1", type: "abort_compaction" });
		expect((await responseFor(conn, "ac1")).success).toBe(true);

		conn.emit({ id: "ab1", type: "abort_branch_summary" });
		expect((await responseFor(conn, "ab1")).success).toBe(true);

		conn.emit({ id: "au1", type: "get_auth_status" });
		const auth = await responseFor(conn, "au1");
		expect(auth.success).toBe(true);
		expect(Array.isArray((auth.data as { oauthProviders: string[] }).oauthProviders)).toBe(true);
	});

	it("answers reload and export_jsonl", async () => {
		const server = await startP2Server();
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "rl1", type: "reload" });
		expect((await responseFor(conn, "rl1")).success).toBe(true);

		conn.emit({ id: "ex1", type: "export_jsonl", outputPath: join(tempDir, "export.jsonl") });
		const exported = await responseFor(conn, "ex1");
		expect(exported.success).toBe(true);
		expect((exported.data as { path: string }).path).toBe(join(tempDir, "export.jsonl"));
		expect(existsSync(join(tempDir, "export.jsonl"))).toBe(true);
	});

	it("answers refresh_models with a response (offline registry may decline)", async () => {
		const server = await startP2Server();
		const conn = new FakeConnection();
		server.attachConnection(conn);

		conn.emit({ id: "rf1", type: "refresh_models" });
		const refreshed = await responseFor(conn, "rf1");
		expect(refreshed.command).toBe("refresh_models");
	});

	it("renames a session file by path", async () => {
		const server = await startP2Server();
		const conn = new FakeConnection();
		server.attachConnection(conn);

		const sessionsDir = join(tempDir, "sessions");
		mkdirSync(sessionsDir, { recursive: true });
		const sessionPath = join(sessionsDir, "rename-me.jsonl");
		writeFileSync(
			sessionPath,
			`${JSON.stringify({ type: "session", id: "rename-me", timestamp: new Date().toISOString(), cwd: tempDir })}\n`,
		);

		conn.emit({ id: "rn1", type: "rename_session", sessionPath, name: "my session" });
		expect((await responseFor(conn, "rn1")).success).toBe(true);

		const reopened = SessionManager.open(sessionPath);
		expect(reopened.getSessionName()).toBe("my session");
	});

	it("navigates the tree without summarization", async () => {
		const server = await startP2Server();
		const conn = new FakeConnection();
		server.attachConnection(conn);

		// Produce some entries first.
		conn.emit({ id: "pr1", type: "prompt", message: "hi" });
		await vi.waitFor(() => expect(conn.sent.some((o) => o.type === "agent_settled")).toBe(true));

		conn.emit({ id: "ge1", type: "get_entries" });
		const entriesResponse = await responseFor(conn, "ge1");
		const { entries, leafId } = entriesResponse.data as {
			entries: Array<{ id: string }>;
			leafId: string | null;
		};
		expect(entries.length).toBeGreaterThan(0);
		expect(leafId).toBeTruthy();

		conn.emit({ id: "nt1", type: "navigate_tree", targetId: leafId! });
		const navigated = await responseFor(conn, "nt1");
		expect(navigated.success).toBe(true);
		expect((navigated.data as { cancelled: boolean }).cancelled).toBe(false);
	});

	it("emits session_changed when the server rebinds to a new session", async () => {
		tempDir = join(tmpdir(), `pi-rpc-p2-rebind-${Date.now()}`);
		mkdirSync(tempDir, { recursive: true });

		const hostA = await createRuntimeHost(tempDir);
		cleanups.push(hostA.cleanup);
		const dirB = join(tmpdir(), `pi-rpc-p2-rebind-b-${Date.now()}`);
		mkdirSync(dirB, { recursive: true });
		const hostB = await createRuntimeHost(dirB);
		cleanups.push(hostB.cleanup);

		const runtimeHost = {
			...hostA.runtimeHost,
			newSession: vi.fn(async () => {
				(runtimeHost as { session: unknown }).session = hostB.session;
				return { cancelled: false };
			}),
		} as unknown as AgentSessionRuntime;

		const server = new RpcServer(runtimeHost, {
			connectionLoss: "grace",
			detachGraceMs: 30_000,
			onShutdown: () => {},
		});
		await server.start();

		const conn = new FakeConnection();
		server.attachConnection(conn);
		// The initial bind happened before attach: no session_changed yet.
		expect(conn.sent.some((o) => o.type === "session_changed")).toBe(false);

		conn.emit({ id: "ns1", type: "new_session" });
		await responseFor(conn, "ns1");

		const changed = conn.sent.filter((o) => o.type === "session_changed");
		expect(changed.length).toBe(1);
		expect(changed[0].sessionId).toBe(hostB.session.sessionId);
		expect(changed[0].cwd).toBe(dirB);
	});
});
