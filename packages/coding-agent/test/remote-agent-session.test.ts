/**
 * E2E tests for the remote-attach facades: a real RpcServer on a unix socket,
 * a real RpcClient, and RemoteAgentSession + RemoteAgentSessionRuntime on top.
 */

import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { Agent } from "@earendil-works/pi-agent-core";
import { type AssistantMessage, type AssistantMessageEvent, EventStream, getModel } from "@earendil-works/pi-ai/compat";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentSession, type AgentSessionEvent } from "../src/core/agent-session.ts";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import { SessionImportFileNotFoundError, SessionImportUnsupportedError } from "../src/core/agent-session-runtime.ts";
import { AuthStorage } from "../src/core/auth-storage.ts";
import type { ExtensionFactory } from "../src/core/extensions/types.ts";
import { MissingSessionCwdError } from "../src/core/session-cwd.ts";
import { SessionManager } from "../src/core/session-manager.ts";
import { SettingsManager } from "../src/core/settings-manager.ts";
import { RemoteAgentSession } from "../src/modes/attach/remote-agent-session.ts";
import { RemoteAgentSessionRuntime } from "../src/modes/attach/remote-runtime.ts";
import { RpcClient } from "../src/modes/rpc/rpc-client.ts";
import { createRpcSocketServer, type RpcSocketServer } from "../src/modes/rpc/rpc-socket-mode.ts";
import { createModelRegistry, getModelRuntime } from "./model-runtime-test-utils.ts";
import { createTestExtensionsResult, createTestResourceLoader } from "./utilities.ts";

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

interface Fixture {
	tempDir: string;
	socketPath: string;
	runtimeHost: AgentSessionRuntime;
	socketServer: RpcSocketServer;
	session: AgentSession;
}

describe("RemoteAgentSession facade", () => {
	const fixtures: Fixture[] = [];
	const clients: RpcClient[] = [];

	afterEach(async () => {
		for (const client of clients.splice(0)) {
			try {
				await client.stop();
			} catch {
				// ignore
			}
		}
		for (const fixture of fixtures.splice(0)) {
			try {
				await fixture.socketServer.close();
			} catch {
				// ignore
			}
			try {
				fixture.session.dispose();
			} catch {
				// ignore
			}
			if (existsSync(fixture.tempDir)) rmSync(fixture.tempDir, { recursive: true });
		}
	});

	async function startFixture(
		options: { extensions?: ExtensionFactory[]; suffix?: string; streamDelayMs?: number; persisted?: boolean } = {},
	): Promise<Fixture> {
		const tempDir = join(
			tmpdir(),
			`pi-remote-facade-${Date.now()}-${options.suffix ?? Math.random().toString(36).slice(2)}`,
		);
		mkdirSync(tempDir, { recursive: true });
		const socketPath = join(tempDir, "agent.sock");

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
					}, options.streamDelayMs ?? 0);
				});
				return stream;
			},
		});

		const settingsManager = SettingsManager.create(tempDir, tempDir);
		const authStorage = AuthStorage.inMemory({ anthropic: { type: "api_key", key: "test-key" } });
		const modelRegistry = await createModelRegistry(authStorage, tempDir);
		const extensionsResult = options.extensions
			? await createTestExtensionsResult(options.extensions, tempDir)
			: undefined;

		const session = new AgentSession({
			agent,
			sessionManager: options.persisted ? SessionManager.create(tempDir) : SessionManager.inMemory(tempDir),
			settingsManager,
			cwd: tempDir,
			modelRuntime: getModelRuntime(modelRegistry),
			resourceLoader: createTestResourceLoader({ extensionsResult }),
		});

		const runtimeHost = {
			session,
			newSession: vi.fn(async () => ({ cancelled: true })),
			switchSession: vi.fn(async () => ({ cancelled: true })),
			fork: vi.fn(async () => ({ cancelled: true, selectedText: "" })),
			dispose: vi.fn(async () => {}),
			setRebindSession: vi.fn(),
		} as unknown as AgentSessionRuntime;

		const socketServer = await createRpcSocketServer(runtimeHost, { socketPath });
		const fixture: Fixture = { tempDir, socketPath, runtimeHost, socketServer, session };
		fixtures.push(fixture);
		return fixture;
	}

	async function connectFacade(fixture: Fixture): Promise<{
		client: RpcClient;
		remote: RemoteAgentSession;
		settingsManager: SettingsManager;
	}> {
		const client = new RpcClient({ socketPath: fixture.socketPath });
		clients.push(client);
		await client.start();
		const settingsManager = SettingsManager.create(fixture.tempDir, fixture.tempDir);
		const remote = await RemoteAgentSession.connect({ client, settingsManager });
		return { client, remote, settingsManager };
	}

	it("mirrors session state at attach", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		expect(remote.sessionId).toBe(fixture.session.sessionId);
		expect(remote.model?.id).toBe("claude-sonnet-4-5");
		expect(remote.isStreaming).toBe(false);
		expect(remote.messages).toEqual([]);
		expect(remote.sessionManager.getCwd()).toBe(fixture.tempDir);
		expect(remote.scopedModels).toEqual([]);
		expect(Array.isArray(remote.getAllTools())).toBe(true);
		expect(typeof remote.systemPrompt).toBe("string");
		expect(remote.systemPrompt.length).toBeGreaterThan(0);
	});

	it("prompts, streams events to subscribers, and mirrors messages", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		const events: string[] = [];
		remote.subscribe((event: AgentSessionEvent) => events.push(event.type));

		await remote.prompt("hello");
		await remote.waitForIdle();

		expect(events).toContain("agent_start");
		expect(events).toContain("agent_end");
		expect(events).toContain("agent_settled");
		expect(remote.isStreaming).toBe(false);
		expect(remote.messages.length).toBeGreaterThan(0);
		expect(remote.getLastAssistantText()).toBe("done");
		expect(remote.state.messages.length).toBe(remote.messages.length);
		expect(remote.getContextUsage()).toBeDefined();
	});

	it("mirrors queue state and clears it", async () => {
		const fixture = await startFixture({ streamDelayMs: 300 });
		const { remote } = await connectFacade(fixture);

		await remote.prompt("first");
		// While streaming, queue a follow-up.
		await remote.followUp("second");
		await vi.waitFor(() => expect(remote.pendingMessageCount).toBeGreaterThan(0));
		expect(remote.getFollowUpMessages()).toContain("second");

		const cleared = await remote.clearQueue();
		expect(cleared.followUp).toContain("second");
		expect(remote.pendingMessageCount).toBe(0);

		await remote.waitForIdle();
	});

	it("updates the mirror on setThinkingLevel and setModel", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		remote.setThinkingLevel("low");
		expect(remote.thinkingLevel).toBe("low");

		const available = await remote.modelRuntime.getAvailable();
		expect(available.length).toBeGreaterThan(0);
		await remote.setModel(available[0]);
		expect(remote.model?.provider).toBe(available[0].provider);
	});

	it("getSessionStats is synchronous and matches the SessionStats shape", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		await remote.prompt("hello");
		await remote.waitForIdle();

		const stats = remote.getSessionStats();
		expect(stats).not.toBeInstanceOf(Promise);
		expect(stats.tokens).toEqual({
			input: expect.any(Number),
			output: expect.any(Number),
			cacheRead: expect.any(Number),
			cacheWrite: expect.any(Number),
			total: expect.any(Number),
		});
		expect(stats.userMessages).toBe(1);
		expect(stats.assistantMessages).toBe(1);
		expect(stats.sessionId).toBe(remote.sessionId);
		expect(typeof stats.cost).toBe("number");
	});

	it("clearQueue and getUserMessagesForForking are synchronous", async () => {
		const fixture = await startFixture({ streamDelayMs: 300 });
		const { remote } = await connectFacade(fixture);

		await remote.prompt("first");
		await remote.followUp("second");
		await vi.waitFor(() => expect(remote.getFollowUpMessages()).toContain("second"));

		const cleared = remote.clearQueue();
		expect(cleared).not.toBeInstanceOf(Promise);
		expect(cleared.followUp).toContain("second");
		await remote.waitForIdle();

		const forking = remote.getUserMessagesForForking();
		expect(forking).not.toBeInstanceOf(Promise);
		expect(forking.some((m) => m.text === "first")).toBe(true);
	});

	it("getTree returns a synchronous SessionTreeNode[] over mirrored entries", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		await remote.prompt("hello");
		await remote.waitForIdle();

		const roots = remote.sessionManager.getTree();
		expect(Array.isArray(roots)).toBe(true);
		expect(roots.length).toBeGreaterThan(0);
		const count = (nodes: typeof roots): number => nodes.reduce((acc, node) => acc + 1 + count(node.children), 0);
		expect(count(roots)).toBe(remote.sessionManager.getEntries().length);
	});

	it("logout and login round-trip over the wire", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		expect(await remote.modelRuntime.listCredentials()).toContainEqual({
			providerId: "anthropic",
			type: "api_key",
		});

		await remote.modelRuntime.logout("anthropic");
		expect(await remote.modelRuntime.listCredentials()).toHaveLength(0);

		const prompts: Array<{ type: string; message: string }> = [];
		await remote.modelRuntime.login("anthropic", "api_key", {
			prompt: async (prompt: { type: string; message: string }) => {
				prompts.push(prompt);
				return "sk-ant-new-key";
			},
			notify: () => {},
		});
		expect(prompts.length).toBeGreaterThan(0);
		expect(await remote.modelRuntime.listCredentials()).toContainEqual({
			providerId: "anthropic",
			type: "api_key",
		});
	});

	it("imports a session file over the wire and rebinds", async () => {
		const fixtureA = await startFixture({ suffix: "import-a", persisted: true });
		const fixtureB = await startFixture({ suffix: "import-b", persisted: true });
		await fixtureB.session.prompt("hello from b");
		const sourceFile = fixtureB.session.sessionManager.getSessionFile();
		if (!sourceFile) throw new Error("fixture session not persisted");
		const sourceContent = readFileSync(sourceFile, "utf8");

		// The fixture's stub host gets a real import: the uploaded file becomes
		// the active session (content-identical to fixture B's session).
		const host = fixtureA.runtimeHost as unknown as {
			session: unknown;
			importFromJsonl: ReturnType<typeof vi.fn>;
		};
		let importedPath: string | undefined;
		host.importFromJsonl = vi.fn(async (path: string) => {
			importedPath = path;
			host.session = fixtureB.session;
			return { cancelled: false };
		});

		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});
		const rebinds: string[] = [];
		runtime.setRebindSession(async (session) => {
			rebinds.push(session.sessionId);
		});

		const uploadPath = join(fixtureA.tempDir, `imported-${Date.now()}.jsonl`);
		writeFileSync(uploadPath, sourceContent, "utf8");

		const result = await runtime.importFromJsonl(uploadPath);
		expect(result.cancelled).toBe(false);

		// Server wrote the upload into the AGENT's session dir (sanitized basename)
		expect(importedPath).toBeDefined();
		expect(basename(importedPath!)).toBe(basename(uploadPath));
		expect(readFileSync(importedPath!, "utf8")).toBe(sourceContent);

		await vi.waitFor(() => expect(rebinds).toContain(fixtureB.session.sessionId));
		expect(remote.sessionId).toBe(fixtureB.session.sessionId);
	});

	it("falls back to agent-side read_file for paths missing client-side", async () => {
		const fixtureA = await startFixture({ suffix: "import-agent", persisted: true });
		const host = fixtureA.runtimeHost as unknown as {
			session: unknown;
			importFromJsonl: ReturnType<typeof vi.fn>;
		};
		host.importFromJsonl = vi.fn(async () => ({ cancelled: false }));

		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});

		// Exists only on the AGENT's filesystem (fixture tempDir), not relative
		// to the test process's cwd.
		writeFileSync(join(fixtureA.tempDir, "agent-only.jsonl"), "{}\n", "utf8");

		const result = await runtime.importFromJsonl("agent-only.jsonl");
		expect(result.cancelled).toBe(false);
		expect(host.importFromJsonl).toHaveBeenCalledOnce();
	});

	it("rejects HTML exports with a helpful message", async () => {
		const fixtureA = await startFixture({ suffix: "import-html", persisted: true });
		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});

		const htmlPath = join(fixtureA.tempDir, "export.html");
		writeFileSync(htmlPath, "<!DOCTYPE html><html>...</html>", "utf8");

		const error = await runtime.importFromJsonl(htmlPath).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(Error);
		expect((error as Error).message).toMatch(/not a session JSONL/i);
	});

	it("rejects an empty session file with a clear message", async () => {
		const fixtureA = await startFixture({ suffix: "import-empty", persisted: true });
		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});

		const emptyPath = join(fixtureA.tempDir, "empty.jsonl");
		writeFileSync(emptyPath, "", "utf8");

		const error = await runtime.importFromJsonl(emptyPath).catch((e: unknown) => e);
		expect((error as Error).message).toMatch(/empty/i);
	});

	it("surfaces agent-side read errors instead of misreporting them as client-side not-found", async () => {
		const fixtureA = await startFixture({ suffix: "import-binerr", persisted: true });
		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});

		// A binary file that exists only on the AGENT's cwd (relative path
		// misses client-side): read_file rejects with "Binary file", which
		// must surface as itself, not as "file not found" at the client path.
		writeFileSync(join(fixtureA.tempDir, "binary-session.jsonl"), Buffer.from([0x89, 0x50, 0x00, 0x01]));

		const error = await runtime.importFromJsonl("binary-session.jsonl").catch((e: unknown) => e);
		expect((error as Error).message).toMatch(/agent host/);
		expect((error as Error).message).toMatch(/binary file/i);
		expect(error).not.toBeInstanceOf(SessionImportFileNotFoundError);
	});

	it("reports wire import failures as unsupported instead of hitting the fatal path", async () => {
		// The stock import handler routes unclassified errors to
		// handleFatalRuntimeError (process.exit) — over the wire that would
		// kill the attach for a recoverable condition.
		const fixtureA = await startFixture({ suffix: "import-wireerr", persisted: true });
		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});

		const uploadPath = join(fixtureA.tempDir, "wire-err.jsonl");
		writeFileSync(uploadPath, "{}\n", "utf8");
		vi.spyOn(client, "importSession").mockRejectedValue(new Error("Unknown command: import_session"));

		const error = await runtime.importFromJsonl(uploadPath).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(SessionImportUnsupportedError);
		expect((error as Error).message).toContain("Unknown command: import_session");
	});

	it("pre-flights the import_session capability with an actionable message", async () => {
		const fixtureA = await startFixture({ suffix: "import-cap", persisted: true });
		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});
		vi.spyOn(client, "hasCapability").mockReturnValue(false);

		const error = await runtime.importFromJsonl("whatever.jsonl").catch((e: unknown) => e);
		expect(error).toBeInstanceOf(SessionImportUnsupportedError);
		expect((error as Error).message).toMatch(/does not support \/import/);
	});

	it("rejects agent-side files too large to read over the wire", async () => {
		const fixtureA = await startFixture({ suffix: "import-trunc", persisted: true });
		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});

		// Just over the 1MB read_file cap, agent-side only.
		writeFileSync(join(fixtureA.tempDir, "big-session.jsonl"), "{}\n".repeat(400_000));

		const error = await runtime.importFromJsonl("big-session.jsonl").catch((e: unknown) => e);
		expect((error as Error).message).toMatch(/too large/i);
	});

	it("modelRuntime.refresh absorbs wire failures into the errors map", async () => {
		// The stock TUI awaits refresh() uncaught in showModelsSelector — a
		// wire failure (e.g. a bounded-out agent-side catalog fetch) must
		// surface via the errors map, never as a rejection.
		const fixture = await startFixture({ suffix: "refresh-err" });
		const { client, remote } = await connectFacade(fixture);
		vi.spyOn(client, "refreshModels").mockRejectedValue(new Error("boom"));

		const result = await remote.modelRuntime.refresh();
		expect(result.aborted).toBe(false);
		expect(result.errors.get("*")?.message).toBe("boom");
	});

	it("dedups a message_end replayed over a refetched snapshot", async () => {
		const fixture = await startFixture({ suffix: "msg-dedup", persisted: true });
		await fixture.session.prompt("dedup check");
		const { remote } = await connectFacade(fixture);

		const mirror = remote as unknown as {
			applyMirror: (event: unknown) => void;
			_messages: unknown[];
			_drainingPending: boolean;
		};
		const before = mirror._messages.length;
		expect(before).toBeGreaterThan(0);
		const last = mirror._messages[before - 1];

		// Replaying the same terminal event while draining the post-refetch
		// queue (same role+timestamp) must not push a duplicate.
		mirror._drainingPending = true;
		mirror.applyMirror({ type: "message_end", message: last });
		mirror._drainingPending = false;
		expect(mirror._messages.length).toBe(before);
	});

	it("dedups several messages replayed from one refetch window", async () => {
		// Two messages completing during a single refetch are BOTH in the
		// fresh snapshot; draining their queued message_ends must skip both
		// (exact match against the whole mirror, not just the tail).
		const fixture = await startFixture({ suffix: "msg-dedup2", persisted: true });
		await fixture.session.prompt("first");
		await fixture.session.prompt("second");
		const { remote } = await connectFacade(fixture);

		const mirror = remote as unknown as {
			applyMirror: (event: unknown) => void;
			_messages: unknown[];
			_drainingPending: boolean;
		};
		const before = mirror._messages.length;
		expect(before).toBeGreaterThan(1);
		const [secondLast, last] = mirror._messages.slice(-2);

		mirror._drainingPending = true;
		mirror.applyMirror({ type: "message_end", message: secondLast });
		mirror.applyMirror({ type: "message_end", message: last });
		mirror._drainingPending = false;
		expect(mirror._messages.length).toBe(before);
	});

	it("keeps same-millisecond messages arriving outside a refetch drain", async () => {
		// The drain-window dedup must NEVER fire on live traffic: two
		// back-to-back tool calls can produce toolResult messages with the
		// same role AND the same Date.now() millisecond.
		const fixture = await startFixture({ suffix: "msg-live", persisted: true });
		await fixture.session.prompt("live check");
		const { remote } = await connectFacade(fixture);

		const mirror = remote as unknown as {
			applyMirror: (event: unknown) => void;
			_messages: unknown[];
		};
		const before = mirror._messages.length;
		const ts = Date.now();
		const toolResult = (id: string) => ({
			role: "toolResult",
			toolCallId: id,
			toolName: "read",
			content: [],
			details: {},
			isError: false,
			timestamp: ts,
		});
		mirror.applyMirror({ type: "message_end", message: toolResult("call-1") });
		mirror.applyMirror({ type: "message_end", message: toolResult("call-2") });
		expect(mirror._messages.length).toBe(before + 2);
	});

	it("maps missingCwd over the wire back to MissingSessionCwdError", async () => {
		const fixtureA = await startFixture({ suffix: "import-cwd", persisted: true });
		const host = fixtureA.runtimeHost as unknown as { importFromJsonl: ReturnType<typeof vi.fn> };
		host.importFromJsonl = vi.fn(async () => {
			throw new MissingSessionCwdError({ sessionCwd: "/gone/for/sure", fallbackCwd: fixtureA.tempDir });
		});

		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});

		const uploadPath = join(fixtureA.tempDir, "imported-cwd.jsonl");
		writeFileSync(uploadPath, "{}\n", "utf8");

		const error = await runtime.importFromJsonl(uploadPath).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(MissingSessionCwdError);
		expect((error as MissingSessionCwdError).issue.sessionCwd).toBe("/gone/for/sure");
	});

	it("mirrors auth capabilities and credentials for /login and /logout", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		const providers = remote.modelRuntime.getProviders();
		const anthropic = providers.find((p) => p.id === "anthropic");
		expect(anthropic).toBeDefined();
		expect(anthropic?.auth.apiKey ?? anthropic?.auth.oauth).toBeTruthy();

		const credentials = await remote.modelRuntime.listCredentials();
		expect(credentials).not.toBeInstanceOf(Promise);
		expect(credentials.some((c) => c.providerId === "anthropic")).toBe(true);
	});

	it("modelRuntime.refresh returns the ModelsRefreshResult shape", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		const controller = new AbortController();
		const result = await remote.modelRuntime.refresh({ signal: controller.signal });
		expect(result.aborted).toBe(false);
		expect(result.errors).toBeInstanceOf(Map);
		expect(result.errors.size).toBe(0);
	});

	it("routes extension_ui_request to the bound TUI uiContext", async () => {
		const confirmations: Array<{ title: string; message: string }> = [];
		const uiProbe: ExtensionFactory = (pi) => {
			pi.on("user_bash", async (_event, ctx) => {
				const ok = await ctx.ui.confirm("Run command?", "bash wants to run");
				confirmations.push({ title: "Run command?", message: String(ok) });
				return undefined;
			});
		};
		const fixture = await startFixture({ extensions: [uiProbe] });
		const { remote } = await connectFacade(fixture);

		await remote.bindExtensions({
			uiContext: {
				select: async () => undefined,
				confirm: async (title: string, message: string) => {
					confirmations.push({ title, message });
					return true;
				},
				input: async () => undefined,
				editor: async () => undefined,
				notify: () => {},
				onTerminalInput: () => () => {},
				setStatus: () => {},
				setWorkingMessage: () => {},
				setWidget: () => {},
				setTitle: () => {},
				setEditorText: () => {},
				pasteToEditor: () => {},
				getEditorText: () => "",
			} as never,
			mode: "tui",
		});

		const result = await remote.executeBash("true");
		expect(result.exitCode).toBe(0);
		await vi.waitFor(() => expect(confirmations.some((c) => c.title === "Run command?")).toBe(true));
	});

	it("streams bash output chunks to executeBash onChunk", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		const chunks: string[] = [];
		const result = await remote.executeBash("printf facade-chunk-test", (chunk) => chunks.push(chunk));
		expect(result.exitCode).toBe(0);
		expect(chunks.join("")).toContain("facade-chunk-test");
	});

	it("rebinds on session_changed after runtime newSession", async () => {
		const fixtureA = await startFixture({ suffix: "a" });
		const fixtureB = await startFixture({ suffix: "b" });

		// runtimeHost that actually swaps sessions on newSession
		const runtimeHost = {
			...fixtureA.runtimeHost,
			newSession: vi.fn(async () => {
				(runtimeHost as { session: unknown }).session = fixtureB.session;
				return { cancelled: false };
			}),
		} as unknown as AgentSessionRuntime;
		fixtures.push({
			tempDir: fixtureA.tempDir,
			socketPath: join(fixtureA.tempDir, "swapped.sock"),
			runtimeHost,
			socketServer: await createRpcSocketServer(runtimeHost, { socketPath: join(fixtureA.tempDir, "swapped.sock") }),
			session: fixtureA.session,
		});
		const swappedPath = join(fixtureA.tempDir, "swapped.sock");

		const client = new RpcClient({ socketPath: swappedPath });
		clients.push(client);
		await client.start();
		const settingsManager = SettingsManager.create(fixtureA.tempDir, fixtureA.tempDir);
		const remote = await RemoteAgentSession.connect({ client, settingsManager });
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});

		const rebinds: string[] = [];
		runtime.setBeforeSessionInvalidate(() => rebinds.push("before"));
		runtime.setRebindSession(async (session) => {
			rebinds.push(`rebind:${session.sessionId}`);
		});

		expect(remote.sessionId).toBe(fixtureA.session.sessionId);
		const result = await runtime.newSession();
		expect(result.cancelled).toBe(false);

		expect(remote.sessionId).toBe(fixtureB.session.sessionId);
		expect(rebinds).toEqual(["before", `rebind:${fixtureB.session.sessionId}`]);
		expect(remote.sessionManager.getCwd()).toBe(fixtureB.tempDir);
	});

	it("reconnects to a restarted agent and rebinds the mirror", async () => {
		const fixtureA = await startFixture({ suffix: "re-a" });
		const fixtureB = await startFixture({ suffix: "re-b" });
		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});
		const rebinds: string[] = [];
		runtime.setRebindSession(async (session) => {
			rebinds.push(`rebind:${session.sessionId}`);
		});

		expect(remote.sessionId).toBe(fixtureA.session.sessionId);

		// Simulate an agent restart: the old server dies, a new one binds the
		// same path holding a different session.
		const socketPath = fixtureA.socketPath;
		const closed = new Promise<void>((resolve) => {
			client.onClose(() => resolve());
		});
		await fixtureA.socketServer.close();
		await closed;
		const replacement = await createRpcSocketServer(fixtureB.runtimeHost, { socketPath });
		fixtures.push({
			tempDir: fixtureB.tempDir,
			socketPath,
			runtimeHost: fixtureB.runtimeHost,
			socketServer: replacement,
			session: fixtureB.session,
		});

		await client.reconnect();
		await runtime.handleReconnect();

		expect(remote.sessionId).toBe(fixtureB.session.sessionId);
		expect(remote.sessionManager.getCwd()).toBe(fixtureB.tempDir);
		expect(rebinds).toEqual([`rebind:${fixtureB.session.sessionId}`]);

		// The new connection is fully usable.
		await remote.prompt("hello after reconnect");
		await remote.waitForIdle();
		expect(remote.getLastAssistantText()).toBe("done");
	});

	it("notifies on server-initiated detach (takeover)", async () => {
		const fixture = await startFixture();
		const { remote } = await connectFacade(fixture);

		const detached: string[] = [];
		remote.onDetached = (reason) => detached.push(reason);

		const second = new RpcClient({ socketPath: fixture.socketPath });
		clients.push(second);
		await second.start();

		await vi.waitFor(() => expect(detached).toEqual(["takeover"]));
	});
});
