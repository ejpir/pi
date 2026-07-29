/**
 * RPC Client for programmatic access to the coding agent.
 *
 * Three transports:
 * - spawn `node dist/cli.js --mode rpc` (default, legacy)
 * - spawn an arbitrary shell command that bridges an RPC stdio stream
 *   (`command` — e.g. `ssh host pi agent --serve-stdio`, `docker exec -i …`)
 * - connect to a unix socket served by `pi --mode rpc --sock PATH`
 *
 * Servers emit a `hello` greeting first; when `requireHello` is set (default
 * for `command`/`socketPath` transports), start() fails on servers that don't
 * greet, surfacing transport junk (ssh banners, wrong binaries) as a clear
 * error instead of protocol corruption.
 */

import { type ChildProcess, spawn } from "node:child_process";
import { Socket } from "node:net";
import type { AgentMessage, ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ImageContent } from "@earendil-works/pi-ai";
import type { AgentSessionEvent, SessionStats } from "../../core/agent-session.ts";
import type { BashResult } from "../../core/bash-executor.ts";
import type { CompactionResult } from "../../core/compaction/index.ts";
import type { ContextUsage, ToolInfo } from "../../core/extensions/types.ts";
import type { SessionEntry, SessionTreeNode } from "../../core/session-manager.ts";
import { attachJsonlLineReader, serializeJsonLine } from "./jsonl.ts";
import type {
	RpcAuthStatus,
	RpcCommand,
	RpcDetachedEvent,
	RpcExtensionErrorEvent,
	RpcExtensionUIRequest,
	RpcExtensionUIResponse,
	RpcHello,
	RpcResources,
	RpcResponse,
	RpcSessionChangedEvent,
	RpcSessionInfo,
	RpcSessionState,
	RpcSlashCommand,
} from "./rpc-types.ts";
import { RPC_PROTOCOL_VERSION } from "./rpc-types.ts";

// ============================================================================
// Types
// ============================================================================

/** Distributive Omit that works with union types */
type DistributiveOmit<T, K extends keyof T> = T extends unknown ? Omit<T, K> : never;

/** RpcCommand without the id field (for internal send) */
type RpcCommandBody = DistributiveOmit<RpcCommand, "id">;

export interface RpcClientOptions {
	/** Path to the CLI entry point (default: searches for dist/cli.js) */
	cliPath?: string;
	/**
	 * Shell command that provides an RPC stdio stream (e.g.
	 * "ssh host pi agent --serve-stdio"). Spawned via `sh -c`. Mutually
	 * exclusive with cliPath/provider/model/args — the command carries
	 * everything.
	 */
	command?: string;
	/** Connect to a unix socket server instead of spawning a process. */
	socketPath?: string;
	/**
	 * Require the server `hello` greeting during start(). Default: true for
	 * `command`/`socketPath` transports, false for spawned cli.js (which may be
	 * an older pi without hello support).
	 */
	requireHello?: boolean;
	/** Override the hello wait timeout (ms). Default 15000. */
	helloTimeoutMs?: number;
	/** Working directory for the agent (spawn transport only) */
	cwd?: string;
	/** Environment variables (spawn transport only) */
	env?: Record<string, string>;
	/** Provider to use (cli.js spawn transport only) */
	provider?: string;
	/** Model ID to use (cli.js spawn transport only) */
	model?: string;
	/** Additional CLI arguments (cli.js spawn transport only) */
	args?: string[];
}

export interface ModelInfo {
	provider: string;
	id: string;
	contextWindow: number;
	reasoning: boolean;
}

/**
 * Events emitted by the server outside of command responses: the live
 * AgentSession event stream plus server-level lifecycle/UI events.
 * Consumers should narrow on `event.type`. Additive; the legacy listener
 * type below stays unchanged for compatibility.
 */
export type RpcServerEvent =
	| AgentSessionEvent
	| RpcSessionChangedEvent
	| RpcDetachedEvent
	| RpcExtensionUIRequest
	| RpcExtensionErrorEvent;

export type RpcEventListener = (event: AgentSessionEvent) => void;

/** Called when the transport closes (process exit or socket close). */
export type RpcCloseListener = (error: Error | null) => void;

const HELLO_TIMEOUT_MS = 15_000;

// ============================================================================
// RPC Client
// ============================================================================

export class RpcClient {
	private process: ChildProcess | null = null;
	private socket: Socket | null = null;
	private writer: { write(line: string): void } | null = null;
	private stopReading: (() => void) | null = null;
	private eventListeners: RpcEventListener[] = [];
	private closeListeners: RpcCloseListener[] = [];
	private pendingRequests: Map<string, { resolve: (response: RpcResponse) => void; reject: (error: Error) => void }> =
		new Map();
	private requestId = 0;
	private stderr = "";
	private exitError: Error | null = null;
	private hello: RpcHello | undefined;
	private helloWaiter: { resolve: (hello: RpcHello) => void; reject: (error: Error) => void } | null = null;
	private options: RpcClientOptions;

	constructor(options: RpcClientOptions = {}) {
		this.options = options;
		if (options.command && (options.cliPath || options.provider || options.model || options.args)) {
			throw new Error("RpcClientOptions: 'command' is mutually exclusive with cliPath/provider/model/args");
		}
		if (options.command && options.socketPath) {
			throw new Error("RpcClientOptions: 'command' and 'socketPath' are mutually exclusive");
		}
	}

	/**
	 * Start the client: spawn the agent process or connect to the socket.
	 */
	async start(): Promise<void> {
		if (this.writer) {
			throw new Error("Client already started");
		}

		this.exitError = null;
		this.hello = undefined;

		if (this.options.socketPath) {
			this.startSocket(this.options.socketPath);
		} else {
			this.startProcess();
		}

		const requireHello = this.options.requireHello ?? Boolean(this.options.command || this.options.socketPath);
		if (requireHello) {
			await this.waitForHello();
		} else {
			// Legacy spawn behavior: give the process a moment to initialize.
			await new Promise((resolve) => setTimeout(resolve, 100));
			if (this.process && this.process.exitCode !== null) {
				const error = this.exitError ?? this.createExitError(this.process.exitCode, this.process.signalCode);
				this.exitError = error;
				throw error;
			}
		}
	}

	private startProcess(): void {
		let childProcess: ChildProcess;

		if (this.options.command) {
			childProcess = spawn("sh", ["-c", this.options.command], {
				cwd: this.options.cwd,
				env: { ...process.env, ...this.options.env },
				stdio: ["pipe", "pipe", "pipe"],
			});
		} else {
			const cliPath = this.options.cliPath ?? "dist/cli.js";
			const args = ["--mode", "rpc"];

			if (this.options.provider) {
				args.push("--provider", this.options.provider);
			}
			if (this.options.model) {
				args.push("--model", this.options.model);
			}
			if (this.options.args) {
				args.push(...this.options.args);
			}

			childProcess = spawn("node", [cliPath, ...args], {
				cwd: this.options.cwd,
				env: { ...process.env, ...this.options.env },
				stdio: ["pipe", "pipe", "pipe"],
			});
		}
		this.process = childProcess;

		// Collect stderr for debugging; pass through so interactive transports
		// (ssh password prompts) and warnings reach the user's terminal.
		childProcess.stderr?.on("data", (data) => {
			this.stderr += data.toString();
			process.stderr.write(data);
		});

		childProcess.once("exit", (code, signal) => {
			if (this.process !== childProcess) return;
			const error = this.createExitError(code, signal);
			this.handleTransportClosed(error);
		});
		childProcess.once("error", (error) => {
			if (this.process !== childProcess) return;
			const processError = new Error(`Agent process error: ${error.message}. Stderr: ${this.stderr}`);
			this.handleTransportClosed(processError);
		});
		childProcess.stdin?.on("error", (error) => {
			if (this.process !== childProcess) return;
			const stdinError =
				this.exitError ?? new Error(`Agent process stdin error: ${error.message}. Stderr: ${this.stderr}`);
			this.handleTransportClosed(stdinError);
		});

		const stdin = childProcess.stdin!;
		this.writer = {
			write: (line) => {
				stdin.write(line);
			},
		};

		this.stopReading = attachJsonlLineReader(childProcess.stdout!, (line) => {
			this.handleLine(line);
		});
	}

	private startSocket(socketPath: string): void {
		const socket = new Socket();
		this.socket = socket;

		socket.once("error", (error) => {
			this.handleTransportClosed(new Error(`Agent socket error: ${error.message}`));
		});
		socket.once("close", (hadError) => {
			if (this.socket !== socket) return;
			this.handleTransportClosed(
				hadError ? (this.exitError ?? new Error("Agent socket closed with an error")) : null,
			);
		});

		this.writer = {
			write: (line) => {
				socket.write(line);
			},
		};

		this.stopReading = attachJsonlLineReader(socket, (line) => {
			this.handleLine(line);
		});

		socket.connect(socketPath);
	}

	private waitForHello(): Promise<RpcHello> {
		if (this.hello) return Promise.resolve(this.hello);
		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				this.helloWaiter = null;
				reject(
					new Error(
						`Timed out waiting for agent hello. The transport may not be running an RPC server. Stderr: ${this.stderr}`,
					),
				);
			}, this.options.helloTimeoutMs ?? HELLO_TIMEOUT_MS);
			this.helloWaiter = {
				resolve: (hello) => {
					clearTimeout(timeout);
					this.helloWaiter = null;
					resolve(hello);
				},
				reject: (error) => {
					clearTimeout(timeout);
					this.helloWaiter = null;
					reject(error);
				},
			};
		});
	}

	/** The server's hello greeting, if one was received. */
	getHello(): RpcHello | undefined {
		return this.hello;
	}

	/** Whether the server advertised a capability in its hello. */
	hasCapability(capability: string): boolean {
		return this.hello?.capabilities.includes(capability) ?? false;
	}

	/**
	 * Re-dial the agent after the socket connection was lost (e.g. the agent
	 * process was restarted). Socket transports only. Event and close
	 * listeners survive the reconnect; in-flight requests were already
	 * rejected when the connection dropped. Resolves after the new
	 * connection's hello handshake completes.
	 */
	async reconnect(): Promise<void> {
		if (!this.options.socketPath) {
			throw new Error("reconnect is only supported for socket transports");
		}
		if (this.socket && !this.socket.destroyed && this.hello) {
			return; // still connected
		}
		this.stopReading?.();
		this.stopReading = null;
		this.socket = null;
		this.writer = null;
		this.exitError = null;
		this.hello = undefined;
		this.stderr = "";

		this.startSocket(this.options.socketPath);
		if (this.options.requireHello ?? true) {
			await this.waitForHello();
		}
	}

	/**
	 * Stop the client.
	 *
	 * Spawn transport: asks the server to shut down gracefully when it
	 * advertised the capability (this kills the remote agent), then falls back
	 * to SIGTERM/SIGKILL.
	 *
	 * Socket transport: detaches (the agent keeps running) and closes the
	 * connection. Use shutdown() instead to terminate the remote agent.
	 */
	async stop(): Promise<void> {
		if (!this.process && !this.socket) return;

		this.stopReading?.();
		this.stopReading = null;

		if (this.socket) {
			const socket = this.socket;
			if (this.hasCapability("detach") && !socket.destroyed) {
				try {
					await Promise.race([this.detach(), new Promise((_, reject) => setTimeout(reject, 1000))]);
				} catch {
					// Best effort.
				}
			}
			socket.destroy();
			this.socket = null;
			this.writer = null;
			this.pendingRequests.clear();
			return;
		}

		const childProcess = this.process!;

		// Graceful shutdown first when the server supports it.
		if (this.hasCapability("shutdown") && childProcess.exitCode === null) {
			try {
				await Promise.race([
					(async () => {
						await this.shutdown();
						await new Promise<void>((resolve) => {
							childProcess.once("exit", () => resolve());
						});
					})(),
					new Promise((_, reject) => setTimeout(reject, 1500)),
				]);
			} catch {
				// Fall through to signals.
			}
		}

		if (childProcess.exitCode === null) {
			childProcess.kill("SIGTERM");
			await new Promise<void>((resolve) => {
				const timeout = setTimeout(() => {
					childProcess.kill("SIGKILL");
					resolve();
				}, 1000);

				childProcess.on("exit", () => {
					clearTimeout(timeout);
					resolve();
				});
			});
		}

		this.process = null;
		this.writer = null;
		this.pendingRequests.clear();
	}

	/**
	 * Subscribe to agent events (and server events like extension UI requests).
	 */
	onEvent(listener: RpcEventListener): () => void {
		this.eventListeners.push(listener);
		return () => {
			const index = this.eventListeners.indexOf(listener);
			if (index !== -1) {
				this.eventListeners.splice(index, 1);
			}
		};
	}

	/**
	 * Subscribe to transport close. Fires on process exit or socket close,
	 * expected or not; `error` is null for a clean close.
	 */
	onClose(listener: RpcCloseListener): () => void {
		this.closeListeners.push(listener);
		return () => {
			const index = this.closeListeners.indexOf(listener);
			if (index !== -1) {
				this.closeListeners.splice(index, 1);
			}
		};
	}

	/**
	 * Get collected stderr output (spawn transport only; useful for debugging).
	 */
	getStderr(): string {
		return this.stderr;
	}

	// =========================================================================
	// Command Methods
	// =========================================================================

	/**
	 * Send a prompt to the agent.
	 * Returns immediately after sending; use onEvent() to receive streaming events.
	 * Use waitForIdle() to wait for completion.
	 */
	async prompt(message: string, images?: ImageContent[]): Promise<void> {
		await this.send({ type: "prompt", message, images });
	}

	/**
	 * Queue a steering message to interrupt the agent mid-run.
	 */
	async steer(message: string, images?: ImageContent[]): Promise<void> {
		await this.send({ type: "steer", message, images });
	}

	/**
	 * Queue a follow-up message to be processed after the agent finishes.
	 */
	async followUp(message: string, images?: ImageContent[]): Promise<void> {
		await this.send({ type: "follow_up", message, images });
	}

	/**
	 * Abort current operation.
	 */
	async abort(): Promise<void> {
		await this.send({ type: "abort" });
	}

	/**
	 * Start a new session, optionally with parent tracking.
	 * @param parentSession - Optional parent session path for lineage tracking
	 * @returns Object with `cancelled: true` if an extension cancelled the new session
	 */
	async newSession(parentSession?: string): Promise<{ cancelled: boolean }> {
		const response = await this.send({ type: "new_session", parentSession });
		return this.getData(response);
	}

	/**
	 * Get current session state.
	 */
	async getState(): Promise<RpcSessionState> {
		const response = await this.send({ type: "get_state" });
		return this.getData(response);
	}

	/**
	 * Set model by provider and ID.
	 */
	async setModel(provider: string, modelId: string): Promise<{ provider: string; id: string }> {
		const response = await this.send({ type: "set_model", provider, modelId });
		return this.getData(response);
	}

	/**
	 * Cycle to next model.
	 */
	async cycleModel(): Promise<{
		model: { provider: string; id: string };
		thinkingLevel: ThinkingLevel;
		isScoped: boolean;
	} | null> {
		const response = await this.send({ type: "cycle_model" });
		return this.getData(response);
	}

	/**
	 * Get list of available models.
	 */
	async getAvailableModels(): Promise<ModelInfo[]> {
		const response = await this.send({ type: "get_available_models" });
		return this.getData<{ models: ModelInfo[] }>(response).models;
	}

	/**
	 * Set thinking level.
	 */
	async setThinkingLevel(level: ThinkingLevel): Promise<void> {
		await this.send({ type: "set_thinking_level", level });
	}

	/**
	 * Cycle thinking level.
	 */
	async cycleThinkingLevel(): Promise<{ level: ThinkingLevel } | null> {
		const response = await this.send({ type: "cycle_thinking_level" });
		return this.getData(response);
	}

	/**
	 * Get list of available thinking levels for the current model.
	 */
	async getAvailableThinkingLevels(): Promise<ThinkingLevel[]> {
		const response = await this.send({ type: "get_available_thinking_levels" });
		return this.getData<{ levels: ThinkingLevel[] }>(response).levels;
	}

	/**
	 * Set steering mode.
	 */
	async setSteeringMode(mode: "all" | "one-at-a-time"): Promise<void> {
		await this.send({ type: "set_steering_mode", mode });
	}

	/**
	 * Set follow-up mode.
	 */
	async setFollowUpMode(mode: "all" | "one-at-a-time"): Promise<void> {
		await this.send({ type: "set_follow_up_mode", mode });
	}

	/**
	 * Compact session context.
	 */
	async compact(customInstructions?: string): Promise<CompactionResult> {
		const response = await this.send({ type: "compact", customInstructions });
		return this.getData(response);
	}

	/**
	 * Set auto-compaction enabled/disabled.
	 */
	async setAutoCompaction(enabled: boolean): Promise<void> {
		await this.send({ type: "set_auto_compaction", enabled });
	}

	/**
	 * Set auto-retry enabled/disabled.
	 */
	async setAutoRetry(enabled: boolean): Promise<void> {
		await this.send({ type: "set_auto_retry", enabled });
	}

	/**
	 * Abort in-progress retry.
	 */
	async abortRetry(): Promise<void> {
		await this.send({ type: "abort_retry" });
	}

	/**
	 * Execute a bash command.
	 */
	async bash(command: string): Promise<BashResult> {
		const response = await this.send({ type: "bash", command });
		return this.getData(response);
	}

	/**
	 * Execute a bash command with a caller-chosen request id. The server's
	 * `bash_execution_update` events carry the originating request id, so
	 * callers can correlate streamed output chunks with this call.
	 */
	async bashWithId(id: string, command: string, excludeFromContext?: boolean): Promise<BashResult> {
		const response = await this.sendWithId(id, { type: "bash", command, excludeFromContext });
		return this.getData(response);
	}

	/**
	 * Abort running bash command.
	 */
	async abortBash(): Promise<void> {
		await this.send({ type: "abort_bash" });
	}

	/**
	 * Get session statistics.
	 */
	async getSessionStats(): Promise<SessionStats> {
		const response = await this.send({ type: "get_session_stats" });
		return this.getData(response);
	}

	/**
	 * Export session to HTML.
	 */
	async exportHtml(outputPath?: string): Promise<{ path: string }> {
		const response = await this.send({ type: "export_html", outputPath });
		return this.getData(response);
	}

	/**
	 * Switch to a different session file.
	 * @returns Object with `cancelled: true` if an extension cancelled the switch
	 */
	async switchSession(sessionPath: string): Promise<{ cancelled: boolean }> {
		const response = await this.send({ type: "switch_session", sessionPath });
		return this.getData(response);
	}

	/**
	 * Fork from a specific message.
	 * @returns Object with `text` (the message text) and `cancelled` (if extension cancelled)
	 */
	async fork(entryId: string): Promise<{ text: string; cancelled: boolean }> {
		const response = await this.send({ type: "fork", entryId });
		return this.getData(response);
	}

	/**
	 * Clone the current active branch into a new session.
	 * @returns Object with `cancelled: true` if an extension cancelled the clone
	 */
	async clone(): Promise<{ cancelled: boolean }> {
		const response = await this.send({ type: "clone" });
		return this.getData(response);
	}

	/**
	 * Get messages available for forking.
	 */
	async getForkMessages(): Promise<Array<{ entryId: string; text: string }>> {
		const response = await this.send({ type: "get_fork_messages" });
		return this.getData<{ messages: Array<{ entryId: string; text: string }> }>(response).messages;
	}

	/**
	 * Get session entries in append order, optionally only those after the `since` entry id.
	 */
	async getEntries(since?: string): Promise<{ entries: SessionEntry[]; leafId: string | null }> {
		const response = await this.send({ type: "get_entries", since });
		return this.getData<{ entries: SessionEntry[]; leafId: string | null }>(response);
	}

	/**
	 * Get the session entry tree.
	 */
	async getTree(): Promise<{ tree: SessionTreeNode[]; leafId: string | null }> {
		const response = await this.send({ type: "get_tree" });
		return this.getData<{ tree: SessionTreeNode[]; leafId: string | null }>(response);
	}

	/**
	 * Get text of last assistant message.
	 */
	async getLastAssistantText(): Promise<string | null> {
		const response = await this.send({ type: "get_last_assistant_text" });
		return this.getData<{ text: string | null }>(response).text;
	}

	/**
	 * Set the session display name.
	 */
	async setSessionName(name: string): Promise<void> {
		await this.send({ type: "set_session_name", name });
	}

	/**
	 * Get all messages in the session.
	 */
	async getMessages(): Promise<AgentMessage[]> {
		const response = await this.send({ type: "get_messages" });
		return this.getData<{ messages: AgentMessage[] }>(response).messages;
	}

	/**
	 * Get available commands (extension commands, prompt templates, skills).
	 */
	async getCommands(): Promise<RpcSlashCommand[]> {
		const response = await this.send({ type: "get_commands" });
		return this.getData<{ commands: RpcSlashCommand[] }>(response).commands;
	}

	// =========================================================================
	// Remote-attach additions (P2)
	// =========================================================================

	/** Estimated context usage of the current session, or null if unknown. */
	async getContextUsage(): Promise<ContextUsage | null> {
		const response = await this.send({ type: "get_context_usage" });
		return this.getData<ContextUsage | null>(response);
	}

	/** The effective system prompt of the current session. */
	async getSystemPrompt(): Promise<string> {
		const response = await this.send({ type: "get_system_prompt" });
		return this.getData<{ systemPrompt: string }>(response).systemPrompt;
	}

	/** Tool definitions (name, description, schema, guidelines) for the current session. */
	async getTools(): Promise<ToolInfo[]> {
		const response = await this.send({ type: "get_tools" });
		return this.getData<{ tools: ToolInfo[] }>(response).tools;
	}

	/** Metadata about loaded resources (skills, prompt templates, themes, extensions, context files). */
	async getResources(): Promise<RpcResources> {
		const response = await this.send({ type: "get_resources" });
		return this.getData<RpcResources>(response);
	}

	/** Replace the scoped model list offered by the cycle-model UI. */
	async setScopedModels(
		models: Array<{ provider: string; id: string; thinkingLevel?: ThinkingLevel }>,
	): Promise<void> {
		await this.send({ type: "set_scoped_models", models });
	}

	/**
	 * Navigate the session tree, optionally with branch summarization.
	 * Returns whether the navigation was cancelled and any editor text to adopt.
	 */
	async navigateTree(
		targetId: string,
		options: {
			summarize?: boolean;
			customInstructions?: string;
			replaceInstructions?: boolean;
			label?: string;
		} = {},
	): Promise<{ cancelled: boolean; editorText?: string }> {
		const response = await this.send({ type: "navigate_tree", targetId, ...options });
		return this.getData<{ cancelled: boolean; editorText?: string }>(response);
	}

	/** Reload settings, resources, and extensions in the agent process. */
	async reload(): Promise<void> {
		await this.send({ type: "reload" });
	}

	/** Export the current session to a JSONL file. Returns the file path on the agent host. */
	async exportJsonl(outputPath?: string): Promise<{ path: string }> {
		const response = await this.send({ type: "export_jsonl", outputPath });
		return this.getData<{ path: string }>(response);
	}

	/** Abort an in-progress compaction. */
	async abortCompaction(): Promise<void> {
		await this.send({ type: "abort_compaction" });
	}

	/** Abort an in-progress branch summarization. */
	async abortBranchSummary(): Promise<void> {
		await this.send({ type: "abort_branch_summary" });
	}

	/** Clear queued steering and follow-up messages. Returns the cleared queues. */
	async clearQueue(): Promise<{ steering: string[]; followUp: string[] }> {
		const response = await this.send({ type: "clear_queue" });
		return this.getData<{ steering: string[]; followUp: string[] }>(response);
	}

	/** Provider ids currently authenticated via OAuth on the agent host. */
	async getAuthStatus(): Promise<RpcAuthStatus> {
		const response = await this.send({ type: "get_auth_status" });
		return this.getData<RpcAuthStatus>(response);
	}

	/** Refresh model availability (network fetch of provider catalogs). */
	async refreshModels(): Promise<void> {
		await this.send({ type: "refresh_models" });
	}

	/** Rename a session by file path (used by the remote session picker). */
	async renameSession(sessionPath: string, name: string): Promise<void> {
		await this.send({ type: "rename_session", sessionPath, name });
	}

	// =========================================================================
	// Lifecycle
	// =========================================================================

	/**
	 * Terminate the remote agent process (requires a server with the
	 * "shutdown" capability).
	 */
	async shutdown(): Promise<void> {
		await this.send({ type: "shutdown" });
	}

	/**
	 * Detach from the remote agent, leaving it running (socket transport;
	 * requires a server with the "detach" capability).
	 */
	async detach(): Promise<void> {
		await this.send({ type: "detach" });
	}

	/**
	 * Respond to an extension UI request (select/confirm/input/editor).
	 */
	respondToExtensionUI(response: RpcExtensionUIResponse): void {
		this.writeLine(serializeJsonLine(response));
	}

	// =========================================================================
	// Filesystem & sessions (agent-side)
	// =========================================================================

	/**
	 * Complete a path prefix against the agent-side filesystem (for @file
	 * completion when attached remotely).
	 */
	async fsComplete(prefix: string, limit?: number): Promise<Array<{ path: string; isDirectory: boolean }>> {
		const response = await this.send({ type: "fs_complete", prefix, limit });
		return this.getData<{ entries: Array<{ path: string; isDirectory: boolean }> }>(response).entries;
	}

	/**
	 * Read a text file from the agent-side filesystem (for file mentions).
	 * Relative paths resolve against the session cwd.
	 */
	async readFile(path: string): Promise<{ path: string; content: string; truncated: boolean }> {
		const response = await this.send({ type: "read_file", path });
		return this.getData(response);
	}

	/**
	 * List sessions known to the agent (for the session picker when attached
	 * remotely). Defaults to sessions of the agent's cwd; `all` lists across
	 * projects.
	 */
	async listSessions(all?: boolean): Promise<RpcSessionInfo[]> {
		const response = await this.send({ type: "list_sessions", all });
		return this.getData<{ sessions: RpcSessionInfo[] }>(response).sessions;
	}

	// =========================================================================
	// Helpers
	// =========================================================================

	/**
	 * Wait for agent to become idle (no streaming).
	 * Resolves when agent_settled event is received.
	 */
	waitForIdle(timeout = 60000): Promise<void> {
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				unsubscribe();
				reject(new Error(`Timeout waiting for agent to become idle. Stderr: ${this.stderr}`));
			}, timeout);

			const unsubscribe = this.onEvent((event) => {
				if (event.type === "agent_settled") {
					clearTimeout(timer);
					unsubscribe();
					resolve();
				}
			});
		});
	}

	/**
	 * Collect events until agent becomes idle.
	 */
	collectEvents(timeout = 60000): Promise<AgentSessionEvent[]> {
		return new Promise((resolve, reject) => {
			const events: AgentSessionEvent[] = [];
			const timer = setTimeout(() => {
				unsubscribe();
				reject(new Error(`Timeout collecting events. Stderr: ${this.stderr}`));
			}, timeout);

			const unsubscribe = this.onEvent((event) => {
				events.push(event);
				if (event.type === "agent_settled") {
					clearTimeout(timer);
					unsubscribe();
					resolve(events);
				}
			});
		});
	}

	/**
	 * Send prompt and wait for completion, returning all events.
	 */
	async promptAndWait(message: string, images?: ImageContent[], timeout = 60000): Promise<AgentSessionEvent[]> {
		const eventsPromise = this.collectEvents(timeout);
		await this.prompt(message, images);
		return eventsPromise;
	}

	// =========================================================================
	// Internal
	// =========================================================================

	private handleLine(line: string): void {
		let data: Record<string, unknown>;
		try {
			data = JSON.parse(line);
		} catch {
			// Tolerate non-JSON junk (transport banners etc.) before hello.
			if (this.hello) {
				// After hello, non-JSON lines indicate protocol corruption; surface them.
				for (const listener of this.eventListeners) {
					listener({ type: "protocol_error", line } as unknown as AgentSessionEvent);
				}
			}
			return;
		}

		try {
			if (data.type === "hello") {
				this.hello = data as unknown as RpcHello;
				if (typeof this.hello.protocol !== "number" || this.hello.protocol > RPC_PROTOCOL_VERSION) {
					const error = new Error(
						`Unsupported RPC protocol version: ${String(this.hello.protocol)} (client supports ${RPC_PROTOCOL_VERSION})`,
					);
					this.helloWaiter?.reject(error);
				} else {
					this.helloWaiter?.resolve(this.hello);
				}
				return;
			}

			// Check if it's a response to a pending request
			if (data.type === "response" && data.id && this.pendingRequests.has(data.id as string)) {
				const pending = this.pendingRequests.get(data.id as string)!;
				this.pendingRequests.delete(data.id as string);
				pending.resolve(data as unknown as RpcResponse);
				return;
			}

			// Otherwise it's an event
			for (const listener of this.eventListeners) {
				listener(data as unknown as AgentSessionEvent);
			}
		} catch {
			// Ignore malformed lines
		}
	}

	private handleTransportClosed(error: Error | null): void {
		if (error) {
			this.exitError = error;
		}
		this.helloWaiter?.reject(error ?? new Error("Transport closed before hello"));
		this.rejectPendingRequests(error ?? new Error("Agent transport closed"));
		for (const listener of this.closeListeners) {
			listener(error);
		}
	}

	private createExitError(code: number | null, signal: NodeJS.Signals | null): Error {
		return new Error(`Agent process exited (code=${code} signal=${signal}). Stderr: ${this.stderr}`);
	}

	private rejectPendingRequests(error: Error): void {
		for (const pending of this.pendingRequests.values()) {
			pending.reject(error);
		}
		this.pendingRequests.clear();
	}

	private writeLine(line: string): void {
		if (!this.writer) {
			throw new Error("Client not started");
		}
		if (this.exitError) {
			throw this.exitError;
		}
		if (this.process && this.process.exitCode !== null) {
			const error = this.createExitError(this.process.exitCode, this.process.signalCode);
			this.exitError = error;
			throw error;
		}
		if (this.socket?.destroyed) {
			const error = new Error("Agent socket is not writable");
			this.exitError = error;
			throw error;
		}
		this.writer.write(line);
	}

	private async send(command: RpcCommandBody): Promise<RpcResponse> {
		return this.sendWithId(`req_${++this.requestId}`, command);
	}

	private async sendWithId(id: string, command: RpcCommandBody): Promise<RpcResponse> {
		const fullCommand = { ...command, id } as RpcCommand;

		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				this.pendingRequests.delete(id);
				reject(new Error(`Timeout waiting for response to ${command.type}. Stderr: ${this.stderr}`));
			}, 30000);

			this.pendingRequests.set(id, {
				resolve: (response) => {
					clearTimeout(timeout);
					resolve(response);
				},
				reject: (error) => {
					clearTimeout(timeout);
					reject(error);
				},
			});

			try {
				this.writeLine(serializeJsonLine(fullCommand));
			} catch (error: unknown) {
				const writeError = error instanceof Error ? error : new Error(String(error));
				const pending = this.pendingRequests.get(id);
				this.pendingRequests.delete(id);
				pending?.reject(writeError);
			}
		});
	}

	private getData<T>(response: RpcResponse): T {
		if (!response.success) {
			const errorResponse = response as Extract<RpcResponse, { success: false }>;
			throw new Error(errorResponse.error);
		}
		// Type assertion: we trust response.data matches T based on the command sent.
		// This is safe because each public method specifies the correct T for its command.
		const successResponse = response as Extract<RpcResponse, { success: true; data: unknown }>;
		return successResponse.data as T;
	}
}
