/**
 * E2E tests for the remote-attach facades: a real RpcServer on a unix socket,
 * a real RpcClient, and RemoteAgentSession + RemoteAgentSessionRuntime on top.
 */

import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { Agent } from "@earendil-works/pi-agent-core";
import { type AssistantMessage, type AssistantMessageEvent, EventStream, getModel } from "@earendil-works/pi-ai/compat";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentSession, type AgentSessionEvent } from "../src/core/agent-session.ts";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import {
	SessionImportError,
	SessionImportFileNotFoundError,
	SessionImportUnsupportedError,
} from "../src/core/agent-session-runtime.ts";
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
		expect(error).toBeInstanceOf(SessionImportError);
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
		expect(error).toBeInstanceOf(SessionImportError);
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
		expect(error).toBeInstanceOf(SessionImportError);
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
		expect(error).toBeInstanceOf(SessionImportError);
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

	it("agent.abort() routes the TUI interrupt path over the wire", async () => {
		const fixture = await startFixture({ suffix: "agent-abort", persisted: true });
		const { remote } = await connectFacade(fixture);
		// The TUI calls this.agent.abort() on Escape; the facade must expose it.
		await remote.agent.abort();
	});

	it("fork with position 'at' uses clone semantics", async () => {
		const fixtureA = await startFixture({ suffix: "clone-at", persisted: true });
		await fixtureA.session.prompt("hello");
		const { client, remote, settingsManager } = await connectFacade(fixtureA);
		const runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: fixtureA.tempDir,
		});
		const leafId = remote.sessionManager.getLeafId();
		expect(leafId).toBeTruthy();

		vi.mocked(fixtureA.runtimeHost.fork).mockResolvedValue({ cancelled: false, selectedText: "" });
		const result = await runtime.fork(leafId!, { position: "at" });
		expect(result.cancelled).toBe(false);
		// The facade routed to the clone command, which re-enters the runtime
		// with position "at" (wire fork defaults to "before" and would fail).
		expect(fixtureA.runtimeHost.fork).toHaveBeenCalledWith(expect.any(String), { position: "at" });
	});

	it("exportToHtml lands on the CLIENT filesystem", async () => {
		const fixtureA = await startFixture({ suffix: "export-local", persisted: true });
		await fixtureA.session.prompt("hello");
		const { remote } = await connectFacade(fixtureA);

		const localPath = join(fixtureA.tempDir, "client-side-export.html");
		const result = await remote.exportToHtml(localPath);
		expect(result).toBe(localPath);
		expect(existsSync(localPath)).toBe(true);
		expect(readFileSync(localPath, "utf8").length).toBeGreaterThan(0);
	});

	it("switch_session surfaces missing cwd as a typed, retriable error", async () => {
		const fixtureA = await startFixture({ suffix: "switch-mcwd", persisted: true });
		const { client } = await connectFacade(fixtureA);
		const { MissingSessionCwdError } = await import("../src/core/session-cwd.ts");

		// The agent side throws the real typed error when no cwdOverride is
		// given and accepts the retry that carries one.
		vi.mocked(fixtureA.runtimeHost.switchSession).mockImplementation(
			async (sessionPath: string, options?: { cwdOverride?: string }) => {
				if (!options?.cwdOverride) {
					throw new MissingSessionCwdError({
						sessionFile: sessionPath,
						sessionCwd: "/nonexistent-moved-cwd-xyz",
						fallbackCwd: fixtureA.tempDir,
					});
				}
				return { cancelled: false };
			},
		);

		const error = await client.switchSession("/sessions/moved.jsonl").catch((e: unknown) => e);
		expect(error).toBeInstanceOf(MissingSessionCwdError);
		expect((error as InstanceType<typeof MissingSessionCwdError>).issue.sessionCwd).toBe(
			"/nonexistent-moved-cwd-xyz",
		);

		// The TUI's retry flow passes cwdOverride — the switch then succeeds.
		const retry = await client.switchSession("/sessions/moved.jsonl", { cwdOverride: fixtureA.tempDir });
		expect(retry.cancelled).toBe(false);
		expect(vi.mocked(fixtureA.runtimeHost.switchSession).mock.calls[1]?.[1]).toEqual({
			cwdOverride: fixtureA.tempDir,
		});
	});

	it("delete_session deletes on the agent host and refuses unsafe targets", async () => {
		const fixtureA = await startFixture({ suffix: "delete-session", persisted: true });
		await fixtureA.session.prompt("hello");
		const { client } = await connectFacade(fixtureA);

		// A second session in the same directory is listed and deletable.
		const sessionDir = fixtureA.session.sessionManager.getSessionDir();
		const copyPath = join(sessionDir, "delete-me.jsonl");
		writeFileSync(copyPath, readFileSync(fixtureA.session.sessionFile!, "utf8"), "utf8");
		const listed = await client.listSessions(false);
		expect(listed.some((s) => s.path === copyPath)).toBe(true);

		await client.deleteSession(copyPath);
		expect(existsSync(copyPath)).toBe(false);

		// Unknown paths and the ACTIVE session are refused.
		await expect(client.deleteSession(join(sessionDir, "not-there.jsonl"))).rejects.toThrow(/not a known session/i);
		await expect(client.deleteSession(fixtureA.session.sessionFile!)).rejects.toThrow(/currently active/i);
	});

	it("a failed import can never destroy a same-named existing session", async () => {
		const fixtureA = await startFixture({ suffix: "import-clash", persisted: true });
		await fixtureA.session.prompt("hello");
		const { client } = await connectFacade(fixtureA);

		// Seed an existing session file under the name the upload will use.
		const sessionDir = fixtureA.session.sessionManager.getSessionDir();
		const clashPath = join(sessionDir, "clash.jsonl");
		const original = readFileSync(fixtureA.session.sessionFile!, "utf8");
		writeFileSync(clashPath, original, "utf8");

		// Faithful importFromJsonl stand-in: sniff like the real runtime.
		fixtureA.runtimeHost.importFromJsonl = (async (p: string) => {
			const trimmed = readFileSync(p, "utf8").trimStart();
			if (!trimmed.startsWith("{")) {
				throw new SessionImportError("Not a session JSONL file — export with /export <file>.jsonl");
			}
			return { cancelled: false };
		}) as unknown as AgentSessionRuntime["importFromJsonl"];

		// Import junk that fails the sniff.
		await expect(
			client.importSession({ content: "<!DOCTYPE html><html>nope</html>\n", fileName: "clash.jsonl" }),
		).rejects.toThrow(/not a session JSONL/i);

		// The pre-existing session is byte-identical and no temp/backup files remain.
		expect(readFileSync(clashPath, "utf8")).toBe(original);
		const leftovers = readdirSync(sessionDir).filter(
			(name) => name.includes(".pi-import-") || name.includes("backup"),
		);
		expect(leftovers).toEqual([]);
	});

	it("provider auth entries are method-shaped, not booleans", async () => {
		const fixtureA = await startFixture({ suffix: "auth-shapes", persisted: true });
		const { remote } = await connectFacade(fixtureA);
		const providers = remote.modelRuntime.getProviders() as Array<{
			id: string;
			auth: { oauth?: unknown; apiKey?: unknown };
		}>;
		for (const provider of providers) {
			if (provider.auth.oauth !== undefined) {
				expect(typeof provider.auth.oauth).toBe("object");
				expect(typeof (provider.auth.oauth as { login?: unknown }).login).toBe("function");
			}
			if (provider.auth.apiKey !== undefined) {
				expect(typeof provider.auth.apiKey).toBe("object");
				expect(typeof (provider.auth.apiKey as { login?: unknown }).login).toBe("function");
			}
		}
		// Anthropic ships API-key auth in the builtin catalog — the shape must
		// survive the wire for the TUI's method?.login check.
		const anthropic = providers.find((p) => p.id === "anthropic");
		if (anthropic?.auth.apiKey !== undefined) {
			expect(typeof anthropic.auth.apiKey).toBe("object");
		}
	});

	it("stamps message_end events with a per-session sequence matching the get_messages high-water mark", async () => {
		const fixture = await startFixture({ suffix: "seq-stamp", persisted: true });
		const { client } = await connectFacade(fixture);

		const seqs: number[] = [];
		client.onEvent((event) => {
			if (event.type === "message_end") {
				seqs.push((event as unknown as { seq?: number }).seq ?? -1);
			}
		});
		await fixture.session.prompt("hello");

		expect(seqs.length).toBeGreaterThan(0);
		expect(seqs.every((seq) => seq > 0)).toBe(true);
		// Monotonic in emission order.
		expect([...seqs].sort((a, b) => a - b)).toEqual(seqs);

		// The snapshot's high-water mark covers exactly the stamped events so
		// far: a client draining queued events can drop seq <= high-water.
		const snapshot = await client.getMessagesWithSeq();
		expect(snapshot.messageSeq).toBe(seqs[seqs.length - 1]);
		expect(snapshot.messages.length).toBeGreaterThan(0);
	});

	it("replays turn openers on mid-turn attach so the TUI shows it is working", async () => {
		// Attaching while the agent is busy must not look idle: the working
		// indicator (agent_start) and compaction indicator (compaction_start)
		// are event-driven in the TUI, so the facade replays the openers when
		// the refetched state says the activity started while we were blind.
		const fixture = await startFixture({ suffix: "resume-openers", persisted: true });
		const { client, remote } = await connectFacade(fixture);

		const seen: string[] = [];
		remote.subscribe((event) => seen.push(event.type));

		const realState = await client.getState();
		const spy = vi
			.spyOn(client, "getState")
			.mockResolvedValue({ ...realState, isStreaming: true, isCompacting: true });
		await remote.refetchAll();
		expect(seen).toContain("agent_start");
		expect(seen).toContain("compaction_start");

		// A second refetch during the SAME turn must not re-fire the openers
		// (the TUI still has its indicators from the first replay).
		seen.length = 0;
		await remote.refetchAll();
		expect(seen).not.toContain("agent_start");
		expect(seen).not.toContain("compaction_start");
		spy.mockRestore();
	});

	it("defers opener replays fired before the first subscriber (attach-mode ordering)", async () => {
		// RemoteAgentSession.connect() refetches BEFORE InteractiveMode
		// subscribes: a mid-turn attach's opener replay must survive having
		// zero listeners at emission time, or the working spinner never shows.
		const fixture = await startFixture({ suffix: "deferred-openers", persisted: true });
		const { client, remote } = await connectFacade(fixture);

		// No subscribe() yet — mirrors connect() completing before the TUI
		// attaches its listener.
		const realState = await client.getState();
		const spy = vi
			.spyOn(client, "getState")
			.mockResolvedValue({ ...realState, isStreaming: true, isCompacting: true });
		await remote.refetchAll();

		const seen: string[] = [];
		remote.subscribe((event) => seen.push(event.type));
		expect(seen).toContain("agent_start");
		expect(seen).toContain("compaction_start");

		// The flush is one-shot: a second subscriber must not see stale openers.
		const second: string[] = [];
		remote.subscribe((event) => second.push(event.type));
		expect(second).toHaveLength(0);
		spy.mockRestore();
	});

	it("cancels deferred openers whose closer arrives before the first subscriber", async () => {
		// The turn can END in the window between connect()'s refetch and the
		// TUI's subscribe. Flushing the deferred agent_start then would turn
		// on a spinner nothing ever turns off — the closer must cancel it.
		const fixture = await startFixture({ suffix: "stale-openers", persisted: true });
		const { client, remote } = await connectFacade(fixture);

		const realState = await client.getState();
		const spy = vi
			.spyOn(client, "getState")
			.mockResolvedValue({ ...realState, isStreaming: true, isCompacting: true });
		await remote.refetchAll();
		spy.mockRestore();

		// Closers arrive while still nobody is subscribed.
		const internals = remote as unknown as { routeEvent: (event: unknown) => void };
		internals.routeEvent({ type: "agent_end", messages: [] });
		internals.routeEvent({ type: "compaction_end" });

		const seen: string[] = [];
		remote.subscribe((event) => seen.push(event.type));
		expect(seen).toHaveLength(0);
	});

	it("defers live openers in the pre-subscribe window (turn starting during attach)", async () => {
		// A turn STARTING between connect() and subscribe must reach the
		// first subscriber like a synthesized replay would — otherwise the
		// original silent-UI bug returns through the live path.
		const fixture = await startFixture({ suffix: "live-openers", persisted: true });
		const { remote } = await connectFacade(fixture);

		const internals = remote as unknown as { routeEvent: (event: unknown) => void };
		internals.routeEvent({ type: "agent_start" });

		const seen: string[] = [];
		remote.subscribe((event) => seen.push(event.type));
		expect(seen).toEqual(["agent_start"]);
		expect(remote.isStreaming).toBe(true);
	});

	it("synthesizes a missed assistant message_start from the first streamed update", async () => {
		// The TUI's message_update handler no-ops without a streaming
		// component; a mid-turn attach therefore needs the opener replayed
		// (with the update's partial content) exactly once per stream.
		const fixture = await startFixture({ suffix: "resume-stream", persisted: true });
		const { remote } = await connectFacade(fixture);

		const seen: string[] = [];
		remote.subscribe((event) => seen.push(event.type));
		const internals = remote as unknown as { routeEvent: (event: unknown) => void };
		const partial = { role: "assistant", content: [{ type: "text", text: "partial" }], timestamp: 1 };

		internals.routeEvent({ type: "message_update", message: partial });
		expect(seen).toEqual(["message_start", "message_update"]);

		// Subsequent updates flow through without another synthesized opener.
		internals.routeEvent({ type: "message_update", message: partial });
		expect(seen).toEqual(["message_start", "message_update", "message_update"]);

		// Stream close resets: the next turn's attach replays the opener again.
		internals.routeEvent({ type: "message_end", message: partial });
		internals.routeEvent({ type: "message_update", message: partial });
		expect(seen.filter((t) => t === "message_start")).toHaveLength(2);
	});

	it("streams every entry exactly once (server taps the manager hook, not the session event)", async () => {
		// Regression guard: the wire entry stream comes from RpcServer's
		// session-manager hook (all entries); the session subscription's
		// entry_appended (custom entries only, upstream semantics) must NOT
		// also be forwarded or those entries would arrive twice.
		const fixture = await startFixture({ suffix: "entry-once", persisted: true });
		const { client, remote } = await connectFacade(fixture);

		const seen: string[] = [];
		client.onEvent((event) => {
			const e = event as { type?: string; entry?: { id?: string } };
			if (e.type === "entry_appended" && e.entry?.id) seen.push(e.entry.id);
		});

		await remote.prompt("stream some entries");
		await remote.waitForIdle();

		expect(seen.length).toBeGreaterThan(0);
		expect(new Set(seen).size).toBe(seen.length);
	});

	it("dedups drain replays by sequence identity against the snapshot high-water mark", async () => {
		const fixture = await startFixture({ suffix: "seq-dedup", persisted: true });
		const { remote } = await connectFacade(fixture);

		const mirror = remote as unknown as {
			applyMirror: (event: unknown) => void;
			_messages: unknown[];
			_drainingPending: boolean;
			_snapshotSeq: number;
		};
		mirror._snapshotSeq = 10;

		const replayed = { role: "user", content: [{ type: "text", text: "old" }], timestamp: 111 };
		const fresh = { role: "user", content: [{ type: "text", text: "new" }], timestamp: 222 };

		mirror._drainingPending = true;
		const before = mirror._messages.length;
		// At-or-below the high-water mark: in the snapshot, skip.
		mirror.applyMirror({ type: "message_end", message: replayed, seq: 10 });
		expect(mirror._messages.length).toBe(before);
		// Above it: completed after the snapshot, apply.
		mirror.applyMirror({ type: "message_end", message: fresh, seq: 11 });
		expect(mirror._messages.length).toBe(before + 1);
		mirror._drainingPending = false;
	});

	it("drops a drained message_end whose seq outran the snapshot that already contains it", async () => {
		// get_messages can observe a message during the await window between
		// agent-core's state append and the server's seq stamp: the snapshot
		// then contains the message but its high-water mark predates the seq.
		// A newer seq alone must not bypass the structural check.
		const fixture = await startFixture({ suffix: "seq-raced", persisted: true });
		const { remote } = await connectFacade(fixture);

		const mirror = remote as unknown as {
			applyMirror: (event: unknown) => void;
			_messages: unknown[];
			_drainingPending: boolean;
			_snapshotSeq: number;
		};
		mirror._snapshotSeq = 10;

		const raced = { role: "user", content: [{ type: "text", text: "raced" }], timestamp: 333 };
		// Snapshot already holds the message even though its stamp is newer.
		mirror._messages.push(structuredClone(raced));

		mirror._drainingPending = true;
		const before = mirror._messages.length;
		mirror.applyMirror({ type: "message_end", message: raced, seq: 11 });
		expect(mirror._messages.length).toBe(before);
		mirror._drainingPending = false;
	});

	it("cycleThinkingLevel is synchronous (the TUI reads the return immediately)", async () => {
		const fixture = await startFixture({ suffix: "cycle-thinking", persisted: true });
		const { remote } = await connectFacade(fixture);

		const mirror = remote as unknown as {
			mirror: { model?: { reasoning: boolean }; thinkingLevel: string };
			_availableThinkingLevels: Array<"off" | "low" | "high">;
		};
		mirror.mirror.model = { reasoning: true };
		mirror._availableThinkingLevels = ["off", "low", "high"];
		mirror.mirror.thinkingLevel = "off";

		// A promise here used to render as "Thinking level: [object Promise]".
		const next = remote.cycleThinkingLevel();
		expect(typeof next).toBe("string");
		expect(next).toBe("low");
		// Optimistic mirror update is synchronous too.
		expect(mirror.mirror.thinkingLevel).toBe("low");
		expect(remote.cycleThinkingLevel()).toBe("high");

		// Models without reasoning support report undefined, matching core.
		mirror.mirror.model = { reasoning: false };
		expect(remote.cycleThinkingLevel()).toBeUndefined();
	});

	it("cycleModel forwards the direction over the wire", async () => {
		const fixture = await startFixture({ suffix: "cycle-model", persisted: true });
		const { client, remote } = await connectFacade(fixture);

		const spy = vi.spyOn(client, "cycleModel").mockResolvedValue(null);
		const result = await remote.cycleModel("backward");
		expect(spy).toHaveBeenCalledWith("backward");
		expect(result).toBeUndefined();
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

	it("covers every projection member the TUI source touches (runtime-miss tripwire)", async () => {
		// The facade's narrowed projections (extensionRunner, resourceLoader,
		// modelRuntime, sessionManager) are outside the Pick-conformance
		// check, so a TUI change that starts touching a new member crashes
		// attach at runtime (see: getMarkdownTransformers after the markdown
		// transform merge). This scans the interactive-mode sources for
		// direct member chains and asserts each exists on a live facade.
		// Locals holding a projection (e.g. this.modelRuntime in
		// model-selector) are matched by their field name.
		const interactiveDir = join(__dirname, "..", "src", "modes", "interactive");
		const sources = readdirSync(interactiveDir, { recursive: true, withFileTypes: true })
			.filter((entry) => entry.isFile() && entry.name.endsWith(".ts"))
			.map((entry) => readFileSync(join(entry.parentPath, entry.name), "utf8"))
			.join("\n");

		const touched = new Map<string, Set<string>>();
		const chainPattern =
			/\b(?:session|services|this)\.(extensionRunner|resourceLoader|modelRuntime|sessionManager)\.([A-Za-z_$][\w$]*)/g;
		for (const match of sources.matchAll(chainPattern)) {
			const [, projection, member] = match;
			if (!touched.has(projection)) touched.set(projection, new Set());
			touched.get(projection)!.add(member);
		}

		// The scan must at least see the member that crashed attach once —
		// if this fails, the regex rotted, not the facade.
		expect(touched.get("extensionRunner")).toBeDefined();
		expect([...touched.get("extensionRunner")!]).toContain("getMarkdownTransformers");

		const fixture = await startFixture({ suffix: "coverage" });
		const { remote } = await connectFacade(fixture);
		const facade = remote as unknown as Record<string, Record<string, unknown>>;

		const missing: string[] = [];
		for (const [projection, members] of touched) {
			for (const member of members) {
				if (facade[projection]?.[member] === undefined) {
					missing.push(`${projection}.${member}`);
				}
			}
		}
		expect(missing, `facade is missing members the TUI touches: ${missing.join(", ")}`).toEqual([]);
	});
});
