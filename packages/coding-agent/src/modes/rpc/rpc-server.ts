/**
 * Transport-independent RPC server core.
 *
 * Owns the agent session binding, command handling, and the extension UI
 * bridge. Transports (stdio, unix socket) adapt their byte streams to the
 * RpcConnection interface and call attachConnection().
 *
 * Lifecycle semantics (see integrations/pi-attach-rfc.md):
 * - Every attachment begins with a `hello` greeting.
 * - `shutdown` terminates the agent process; `detach` ends only the client
 *   attachment and leaves the agent running.
 * - After an explicit `detach`, extension UI requests auto-resolve
 *   immediately (headless behavior).
 * - When a connection is *lost* (not detached), pending and new extension UI
 *   requests are held for a grace window (detachGraceMs) and re-emitted to a
 *   reconnecting client; they auto-resolve only when the window expires.
 * - A second attach takes over: the previous client is notified and dropped.
 */

import * as crypto from "node:crypto";
import { VERSION } from "../../config.ts";
import type { AgentSessionRuntime } from "../../core/agent-session-runtime.ts";
import type {
	ExtensionUIContext,
	ExtensionUIDialogOptions,
	ExtensionWidgetOptions,
	WorkingIndicatorOptions,
} from "../../core/extensions/index.ts";
import { type SessionInfo, SessionManager } from "../../core/session-manager.ts";
import { type Theme, theme } from "../interactive/theme/theme.ts";
import { completePaths, readSessionFile } from "./fs-commands.ts";
import {
	RPC_CAPABILITIES,
	RPC_PROTOCOL_VERSION,
	type RpcCommand,
	type RpcExtensionUIRequest,
	type RpcExtensionUIResponse,
	type RpcHello,
	type RpcResponse,
	type RpcSessionInfo,
	type RpcSessionState,
	type RpcSlashCommand,
} from "./rpc-types.ts";

/** A single attached client's byte stream, adapted by the transport. */
export interface RpcConnection {
	/** Serialize and write one protocol object as a JSON line. */
	send(obj: unknown): void;
	/** Subscribe to incoming JSON lines. */
	onLine(cb: (line: string) => void): void;
	/** Called once when the transport closes (client gone). */
	onClose(cb: () => void): void;
	/** Close this connection. */
	close(): void;
	/** Flush buffered writes (best effort). */
	flush?(): Promise<void>;
}

export interface RpcServerOptions {
	/**
	 * How to react when the client connection is lost without a `detach`:
	 * - "shutdown": terminate the server (stdio semantics — the client owns us)
	 * - "grace": hold extension UI requests for `detachGraceMs` awaiting reconnect
	 */
	connectionLoss: "shutdown" | "grace";
	/** Grace window for connection loss. Default 30s. Only for "grace" mode. */
	detachGraceMs?: number;
	/** Terminate the process/server. Called once on shutdown. */
	onShutdown: (exitCode: number) => void | Promise<void>;
}

const DEFAULT_DETACH_GRACE_MS = 30_000;

interface PendingExtensionRequest {
	resolvePromise: (value: unknown) => void;
	rejectPromise: (error: Error) => void;
	request: RpcExtensionUIRequest;
	parseResponse: (response: RpcExtensionUIResponse) => unknown;
	defaultValue: unknown;
	timeoutMs?: number;
	timeoutId?: ReturnType<typeof setTimeout>;
	graceId?: ReturnType<typeof setTimeout>;
	/** Whether the request was already emitted to a (since lost) client. */
	emitted: boolean;
}

export class RpcServer {
	private session;
	private connection: RpcConnection | null = null;
	private explicitlyDetached = false;
	private pendingExtensionRequests = new Map<string, PendingExtensionRequest>();
	private unsubscribe: (() => void) | undefined;
	private unsubscribeBackpressure: (() => void) | undefined;
	private shutdownRequested = false;
	private shuttingDown = false;
	private detachGraceMs: number;
	private runtimeHost: AgentSessionRuntime;
	private options: RpcServerOptions;

	constructor(runtimeHost: AgentSessionRuntime, options: RpcServerOptions) {
		this.runtimeHost = runtimeHost;
		this.options = options;
		this.session = runtimeHost.session;
		this.detachGraceMs = options.detachGraceMs ?? DEFAULT_DETACH_GRACE_MS;
	}

	async start(): Promise<void> {
		this.runtimeHost.setRebindSession(async () => {
			await this.rebindSession();
		});
		await this.rebindSession();
	}

	// =========================================================================
	// Connection lifecycle
	// =========================================================================

	/** Attach a client. Takes over from any currently attached client. */
	attachConnection(connection: RpcConnection): void {
		const previous = this.connection;
		if (previous && previous !== connection) {
			try {
				previous.send({ type: "detached", reason: "takeover" });
			} catch {
				// Previous client already gone.
			}
			previous.close();
		}

		this.connection = connection;
		this.explicitlyDetached = false;

		connection.onLine((line) => {
			void this.handleLine(connection, line);
		});
		connection.onClose(() => {
			this.onConnectionClosed(connection);
		});

		const hello: RpcHello = {
			type: "hello",
			protocol: RPC_PROTOCOL_VERSION,
			version: VERSION,
			sessionId: this.session.sessionId,
			cwd: this.session.sessionManager.getCwd(),
			capabilities: [...RPC_CAPABILITIES],
		};
		connection.send(hello);

		// A (re)connecting client receives all outstanding UI requests.
		for (const entry of this.pendingExtensionRequests.values()) {
			this.clearGrace(entry);
			this.deliver(entry);
		}
	}

	private onConnectionClosed(connection: RpcConnection): void {
		if (connection !== this.connection) return;
		this.connection = null;

		if (this.options.connectionLoss === "shutdown") {
			void this.initiateShutdown(0);
			return;
		}

		if (this.explicitlyDetached) {
			// Explicit detach: auto-resolve outstanding and future requests.
			for (const entry of this.pendingExtensionRequests.values()) {
				this.settle(entry, entry.defaultValue);
			}
			return;
		}

		// Connection lost: hold requests for the grace window.
		for (const entry of this.pendingExtensionRequests.values()) {
			this.armGrace(entry);
		}
	}

	/** Send an event/response to the attached client, if any. */
	private output(obj: object): void {
		this.connection?.send(obj);
	}

	// =========================================================================
	// Extension UI bridge
	// =========================================================================

	private settle(entry: PendingExtensionRequest, value: unknown): void {
		if (entry.timeoutId) clearTimeout(entry.timeoutId);
		this.clearGrace(entry);
		this.pendingExtensionRequests.delete(entry.request.id);
		entry.resolvePromise(value);
	}

	private clearGrace(entry: PendingExtensionRequest): void {
		if (entry.graceId) {
			clearTimeout(entry.graceId);
			entry.graceId = undefined;
		}
	}

	private armGrace(entry: PendingExtensionRequest): void {
		this.clearGrace(entry);
		entry.graceId = setTimeout(() => {
			this.settle(entry, entry.defaultValue);
		}, this.detachGraceMs);
	}

	/** Emit a request to the attached client, arming the extension timeout. */
	private deliver(entry: PendingExtensionRequest): void {
		if (entry.timeoutId) clearTimeout(entry.timeoutId);
		if (entry.timeoutMs) {
			entry.timeoutId = setTimeout(() => {
				this.settle(entry, entry.defaultValue);
			}, entry.timeoutMs);
		}
		entry.emitted = true;
		this.output(entry.request);
	}

	/** Route a request per the current attachment state. */
	private deliverOrHold(entry: PendingExtensionRequest): void {
		if (this.connection) {
			this.deliver(entry);
		} else if (this.explicitlyDetached || this.options.connectionLoss === "shutdown") {
			// Headless: behave as RPC mode always has — auto-resolve.
			this.settle(entry, entry.defaultValue);
		} else {
			// Detached via connection loss: hold for reconnect within grace.
			this.armGrace(entry);
		}
	}

	private createDialogPromise<T>(
		opts: ExtensionUIDialogOptions | undefined,
		defaultValue: T,
		request: Record<string, unknown>,
		parseResponse: (response: RpcExtensionUIResponse) => T,
	): Promise<T> {
		if (opts?.signal?.aborted) return Promise.resolve(defaultValue);

		const id = crypto.randomUUID();
		return new Promise<T>((resolve, reject) => {
			const entry: PendingExtensionRequest = {
				resolvePromise: resolve as (value: unknown) => void,
				rejectPromise: reject,
				request: { type: "extension_ui_request", id, ...request } as RpcExtensionUIRequest,
				parseResponse: parseResponse as (response: RpcExtensionUIResponse) => unknown,
				defaultValue,
				timeoutMs: opts?.timeout,
				emitted: false,
			};
			this.pendingExtensionRequests.set(id, entry);

			opts?.signal?.addEventListener(
				"abort",
				() => {
					this.settle(entry, defaultValue);
				},
				{ once: true },
			);

			this.deliverOrHold(entry);
		});
	}

	private createExtensionUIContext(): ExtensionUIContext {
		return {
			select: (title, options, opts) =>
				this.createDialogPromise(
					opts,
					undefined,
					{ method: "select", title, options, timeout: opts?.timeout },
					(r) => ("cancelled" in r && r.cancelled ? undefined : "value" in r ? r.value : undefined),
				),

			confirm: (title, message, opts) =>
				this.createDialogPromise(opts, false, { method: "confirm", title, message, timeout: opts?.timeout }, (r) =>
					"cancelled" in r && r.cancelled ? false : "confirmed" in r ? r.confirmed : false,
				),

			input: (title, placeholder, opts) =>
				this.createDialogPromise(
					opts,
					undefined,
					{ method: "input", title, placeholder, timeout: opts?.timeout },
					(r) => ("cancelled" in r && r.cancelled ? undefined : "value" in r ? r.value : undefined),
				),

			notify: (message: string, type?: "info" | "warning" | "error"): void => {
				this.output({
					type: "extension_ui_request",
					id: crypto.randomUUID(),
					method: "notify",
					message,
					notifyType: type,
				});
			},

			onTerminalInput: (): (() => void) => {
				// Raw terminal input not supported in RPC mode
				return () => {};
			},

			setStatus: (key: string, text: string | undefined): void => {
				this.output({
					type: "extension_ui_request",
					id: crypto.randomUUID(),
					method: "setStatus",
					statusKey: key,
					statusText: text,
				});
			},

			setWorkingMessage: (_message?: string): void => {},
			setWorkingVisible: (_visible: boolean): void => {},
			setWorkingIndicator: (_options?: WorkingIndicatorOptions): void => {},
			setHiddenThinkingLabel: (_label?: string): void => {},

			setWidget: (key: string, content: unknown, options?: ExtensionWidgetOptions): void => {
				// Only support string arrays in RPC mode - factory functions are ignored
				if (content === undefined || Array.isArray(content)) {
					this.output({
						type: "extension_ui_request",
						id: crypto.randomUUID(),
						method: "setWidget",
						widgetKey: key,
						widgetLines: content as string[] | undefined,
						widgetPlacement: options?.placement,
					});
				}
			},

			setFooter: (_factory: unknown): void => {},
			setHeader: (_factory: unknown): void => {},

			setTitle: (title: string): void => {
				this.output({
					type: "extension_ui_request",
					id: crypto.randomUUID(),
					method: "setTitle",
					title,
				});
			},

			custom: async () => undefined as never,

			pasteToEditor: (text: string): void => {
				this.createExtensionUIContext().setEditorText(text);
			},

			setEditorText: (text: string): void => {
				this.output({
					type: "extension_ui_request",
					id: crypto.randomUUID(),
					method: "set_editor_text",
					text,
				});
			},

			getEditorText: (): string => {
				// Synchronous method can't wait for RPC response
				return "";
			},

			editor: async (title: string, prefill?: string): Promise<string | undefined> => {
				return this.createDialogPromise(undefined, undefined, { method: "editor", title, prefill }, (r) =>
					"cancelled" in r && r.cancelled ? undefined : "value" in r ? r.value : undefined,
				);
			},

			addAutocompleteProvider: (): void => {},
			setEditorComponent: (): void => {},
			getEditorComponent: () => undefined,

			get theme() {
				return theme;
			},

			getAllThemes: () => [],
			getTheme: (_name: string) => undefined,
			setTheme: (_theme: string | Theme) => ({
				success: false as const,
				error: "Theme switching not supported in RPC mode",
			}),

			getToolsExpanded: () => false,
			setToolsExpanded: (_expanded: boolean): void => {},
		};
	}

	private rebindSession = async (): Promise<void> => {
		this.session = this.runtimeHost.session;
		const session = this.session;
		await session.bindExtensions({
			uiContext: this.createExtensionUIContext(),
			mode: "rpc",
			commandContextActions: {
				waitForIdle: () => session.waitForIdle(),
				newSession: async (options) => this.runtimeHost.newSession(options),
				fork: async (entryId, forkOptions) => {
					const result = await this.runtimeHost.fork(entryId, forkOptions);
					return { cancelled: result.cancelled };
				},
				navigateTree: async (targetId, options) => {
					const result = await session.navigateTree(targetId, {
						summarize: options?.summarize,
						customInstructions: options?.customInstructions,
						replaceInstructions: options?.replaceInstructions,
						label: options?.label,
					});
					return { cancelled: result.cancelled };
				},
				switchSession: async (sessionPath, options) => {
					return this.runtimeHost.switchSession(sessionPath, options);
				},
				reload: async () => {
					await session.reload();
				},
			},
			shutdownHandler: () => {
				this.shutdownRequested = true;
			},
			onError: (err) => {
				this.output({
					type: "extension_error",
					extensionPath: err.extensionPath,
					event: err.event,
					error: err.error,
				});
			},
		});

		this.unsubscribe?.();
		this.unsubscribeBackpressure?.();
		this.unsubscribe = session.subscribe((event) => {
			this.output(event);
			if (event.type === "agent_settled") {
				void this.checkShutdownRequested();
			}
		});
		// Backpressure hook is installed by the stdio adapter (output-guard);
		// transports with their own drain handling can subscribe similarly.

		// Tell attached clients the session identity changed so mirrors can
		// refetch. Emitted after the new listeners are installed; dropped
		// silently when no client is attached (e.g. the initial bind).
		this.output({
			type: "session_changed",
			sessionId: session.sessionManager.getSessionId(),
			cwd: session.sessionManager.getCwd(),
		});
	};

	/** Let a transport subscribe to raw agent events (e.g. for backpressure). */
	subscribeAgentEvents(listener: () => void | Promise<void>): () => void {
		return this.session.agent.subscribe(listener);
	}

	// =========================================================================
	// Command handling
	// =========================================================================

	private success<T extends RpcCommand["type"]>(
		id: string | undefined,
		command: T,
		data?: object | null,
	): RpcResponse {
		if (data === undefined) {
			return { id, type: "response", command, success: true } as RpcResponse;
		}
		return { id, type: "response", command, success: true, data } as RpcResponse;
	}

	private error(id: string | undefined, command: string, message: string): RpcResponse {
		return { id, type: "response", command, success: false, error: message };
	}

	private async handleLine(connection: RpcConnection, line: string): Promise<void> {
		let parsed: unknown;
		try {
			parsed = JSON.parse(line);
		} catch (parseError: unknown) {
			connection.send(
				this.error(
					undefined,
					"parse",
					`Failed to parse command: ${parseError instanceof Error ? parseError.message : String(parseError)}`,
				),
			);
			return;
		}

		// Extension UI responses
		if (
			typeof parsed === "object" &&
			parsed !== null &&
			"type" in parsed &&
			parsed.type === "extension_ui_response"
		) {
			const response = parsed as RpcExtensionUIResponse;
			const pending = this.pendingExtensionRequests.get(response.id);
			if (pending) {
				const value = pending.parseResponse(response);
				this.settle(pending, value);
			}
			return;
		}

		const command = parsed as RpcCommand;
		try {
			const response = await this.handleCommand(connection, command);
			if (response && this.connection === connection) {
				connection.send(response);
			}
			await this.checkShutdownRequested();
		} catch (commandError: unknown) {
			connection.send(
				this.error(
					command.id,
					command.type,
					commandError instanceof Error ? commandError.message : String(commandError),
				),
			);
		}
	}

	private async handleCommand(connection: RpcConnection, command: RpcCommand): Promise<RpcResponse | undefined> {
		const id = command.id;
		const session = this.session;

		switch (command.type) {
			// =================================================================
			// Prompting
			// =================================================================

			case "prompt": {
				// Start prompt handling immediately, but emit the authoritative response only after
				// prompt preflight succeeds. Queued and immediately handled prompts also count as success.
				let preflightSucceeded = false;
				void session
					.prompt(command.message, {
						images: command.images,
						streamingBehavior: command.streamingBehavior,
						source: "rpc",
						preflightResult: (didSucceed) => {
							if (didSucceed) {
								preflightSucceeded = true;
								this.output(this.success(id, "prompt"));
							}
						},
					})
					.catch((e) => {
						if (!preflightSucceeded) {
							this.output(this.error(id, "prompt", e.message));
						}
					});
				return undefined;
			}

			case "steer": {
				await session.steer(command.message, command.images);
				return this.success(id, "steer");
			}

			case "follow_up": {
				await session.followUp(command.message, command.images);
				return this.success(id, "follow_up");
			}

			case "abort": {
				await session.abort();
				return this.success(id, "abort");
			}

			case "new_session": {
				const options = command.parentSession ? { parentSession: command.parentSession } : undefined;
				const result = await this.runtimeHost.newSession(options);
				if (!result.cancelled) {
					await this.rebindSession();
				}
				return this.success(id, "new_session", result);
			}

			// =================================================================
			// State
			// =================================================================

			case "get_state": {
				const state: RpcSessionState = {
					model: session.model,
					thinkingLevel: session.thinkingLevel,
					isStreaming: session.isStreaming,
					isCompacting: session.isCompacting,
					steeringMode: session.steeringMode,
					followUpMode: session.followUpMode,
					scopedModels: session.scopedModels.map((entry) => ({
						model: entry.model,
						thinkingLevel: entry.thinkingLevel,
					})),
					sessionFile: session.sessionFile,
					sessionId: session.sessionId,
					sessionName: session.sessionName,
					autoCompactionEnabled: session.autoCompactionEnabled,
					messageCount: session.messages.length,
					pendingMessageCount: session.pendingMessageCount,
				};
				return this.success(id, "get_state", state);
			}

			// =================================================================
			// Model
			// =================================================================

			case "set_model": {
				const models = await session.modelRuntime.getAvailable();
				const model = models.find((m) => m.provider === command.provider && m.id === command.modelId);
				if (!model) {
					return this.error(id, "set_model", `Model not found: ${command.provider}/${command.modelId}`);
				}
				await session.setModel(model);
				return this.success(id, "set_model", model);
			}

			case "cycle_model": {
				const result = await session.cycleModel();
				if (!result) {
					return this.success(id, "cycle_model", null);
				}
				return this.success(id, "cycle_model", result);
			}

			case "get_available_models": {
				const models = await session.modelRuntime.getAvailable();
				return this.success(id, "get_available_models", { models });
			}

			// =================================================================
			// Thinking
			// =================================================================

			case "set_thinking_level": {
				session.setThinkingLevel(command.level);
				return this.success(id, "set_thinking_level");
			}

			case "cycle_thinking_level": {
				const level = session.cycleThinkingLevel();
				if (!level) {
					return this.success(id, "cycle_thinking_level", null);
				}
				return this.success(id, "cycle_thinking_level", { level });
			}

			case "get_available_thinking_levels": {
				const levels = session.getAvailableThinkingLevels();
				return this.success(id, "get_available_thinking_levels", { levels });
			}

			// =================================================================
			// Queue Modes
			// =================================================================

			case "set_steering_mode": {
				session.setSteeringMode(command.mode);
				return this.success(id, "set_steering_mode");
			}

			case "set_follow_up_mode": {
				session.setFollowUpMode(command.mode);
				return this.success(id, "set_follow_up_mode");
			}

			// =================================================================
			// Compaction
			// =================================================================

			case "compact": {
				const result = await session.compact(command.customInstructions);
				return this.success(id, "compact", result);
			}

			case "set_auto_compaction": {
				session.setAutoCompactionEnabled(command.enabled);
				return this.success(id, "set_auto_compaction");
			}

			// =================================================================
			// Retry
			// =================================================================

			case "set_auto_retry": {
				session.setAutoRetryEnabled(command.enabled);
				return this.success(id, "set_auto_retry");
			}

			case "abort_retry": {
				session.abortRetry();
				return this.success(id, "abort_retry");
			}

			// =================================================================
			// Bash
			// =================================================================

			case "bash": {
				const eventResult = await session.extensionRunner.emitUserBash({
					type: "user_bash",
					command: command.command,
					excludeFromContext: command.excludeFromContext ?? false,
					cwd: session.sessionManager.getCwd(),
				});

				if (eventResult?.result) {
					session.recordBashResult(command.command, eventResult.result, {
						excludeFromContext: command.excludeFromContext,
					});
					return this.success(id, "bash", eventResult.result);
				}

				const result = await session.executeBash(command.command, undefined, {
					excludeFromContext: command.excludeFromContext,
					id,
					operations: eventResult?.operations,
				});
				return this.success(id, "bash", result);
			}

			case "abort_bash": {
				session.abortBash();
				return this.success(id, "abort_bash");
			}

			// =================================================================
			// Session
			// =================================================================

			case "get_session_stats": {
				const stats = session.getSessionStats();
				return this.success(id, "get_session_stats", stats);
			}

			case "export_html": {
				const path = await session.exportToHtml(command.outputPath);
				return this.success(id, "export_html", { path });
			}

			case "switch_session": {
				const result = await this.runtimeHost.switchSession(command.sessionPath);
				if (!result.cancelled) {
					await this.rebindSession();
				}
				return this.success(id, "switch_session", result);
			}

			case "fork": {
				const result = await this.runtimeHost.fork(command.entryId);
				if (!result.cancelled) {
					await this.rebindSession();
				}
				return this.success(id, "fork", { text: result.selectedText, cancelled: result.cancelled });
			}

			case "clone": {
				const leafId = session.sessionManager.getLeafId();
				if (!leafId) {
					return this.error(id, "clone", "Cannot clone session: no current entry selected");
				}
				const result = await this.runtimeHost.fork(leafId, { position: "at" });
				if (!result.cancelled) {
					await this.rebindSession();
				}
				return this.success(id, "clone", { cancelled: result.cancelled });
			}

			case "get_fork_messages": {
				const messages = session.getUserMessagesForForking();
				return this.success(id, "get_fork_messages", { messages });
			}

			case "get_entries": {
				const sessionManager = session.sessionManager;
				let entries = sessionManager.getEntries();
				if (command.since !== undefined) {
					const sinceIndex = entries.findIndex((e) => e.id === command.since);
					if (sinceIndex === -1) {
						return this.error(id, "get_entries", `Entry not found: ${command.since}`);
					}
					entries = entries.slice(sinceIndex + 1);
				}
				return this.success(id, "get_entries", { entries, leafId: sessionManager.getLeafId() });
			}

			case "get_tree": {
				const sessionManager = session.sessionManager;
				return this.success(id, "get_tree", { tree: sessionManager.getTree(), leafId: sessionManager.getLeafId() });
			}

			case "get_last_assistant_text": {
				const text = session.getLastAssistantText();
				return this.success(id, "get_last_assistant_text", { text });
			}

			case "set_session_name": {
				const name = command.name.trim();
				if (!name) {
					return this.error(id, "set_session_name", "Session name cannot be empty");
				}
				session.setSessionName(name);
				return this.success(id, "set_session_name");
			}

			// =================================================================
			// Messages
			// =================================================================

			case "get_messages": {
				return this.success(id, "get_messages", { messages: session.messages });
			}

			// =================================================================
			// Commands (available for invocation via prompt)
			// =================================================================

			case "get_commands": {
				const commands: RpcSlashCommand[] = [];

				for (const command of session.extensionRunner.getRegisteredCommands()) {
					commands.push({
						name: command.invocationName,
						description: command.description,
						source: "extension",
						sourceInfo: command.sourceInfo,
					});
				}

				for (const template of session.promptTemplates) {
					commands.push({
						name: template.name,
						description: template.description,
						source: "prompt",
						sourceInfo: template.sourceInfo,
						argumentHint: template.argumentHint,
					});
				}

				for (const skill of session.resourceLoader.getSkills().skills) {
					commands.push({
						name: `skill:${skill.name}`,
						description: skill.description,
						source: "skill",
						sourceInfo: skill.sourceInfo,
					});
				}

				return this.success(id, "get_commands", { commands });
			}

			// =================================================================
			// Remote-attach additions (P2)
			// =================================================================

			case "get_context_usage": {
				return this.success(id, "get_context_usage", session.getContextUsage() ?? null);
			}

			case "get_system_prompt": {
				return this.success(id, "get_system_prompt", { systemPrompt: session.systemPrompt });
			}

			case "get_tools": {
				return this.success(id, "get_tools", { tools: session.getAllTools() });
			}

			case "get_resources": {
				const resourceLoader = session.resourceLoader;
				const extensionsResult = resourceLoader.getExtensions();
				return this.success(id, "get_resources", {
					skills: resourceLoader.getSkills().skills,
					prompts: session.promptTemplates.map(({ content: _content, ...meta }) => meta),
					themes: resourceLoader.getThemes().themes,
					extensions: extensionsResult.extensions.map((extension) => ({
						path: extension.path,
						sourceInfo: extension.sourceInfo,
						hidden: extension.hidden,
					})),
					extensionErrors: extensionsResult.errors,
					agentsFiles: resourceLoader.getAgentsFiles().agentsFiles.map((agentsFile) => ({
						path: agentsFile.path,
					})),
					systemPromptSource: resourceLoader.getSystemPromptSource(),
					appendSystemPromptSources: resourceLoader.getAppendSystemPromptSources(),
				});
			}

			case "set_scoped_models": {
				const availableModels = await session.modelRuntime.getAvailable();
				const scoped = command.models.map((entry) => {
					const model = availableModels.find((m) => m.provider === entry.provider && m.id === entry.id);
					if (!model) {
						throw new Error(`Unknown model: ${entry.provider}/${entry.id}`);
					}
					return { model, thinkingLevel: entry.thinkingLevel };
				});
				session.setScopedModels(scoped);
				return this.success(id, "set_scoped_models");
			}

			case "navigate_tree": {
				const result = await session.navigateTree(command.targetId, {
					summarize: command.summarize,
					customInstructions: command.customInstructions,
					replaceInstructions: command.replaceInstructions,
					label: command.label,
				});
				return this.success(id, "navigate_tree", {
					cancelled: result.cancelled,
					editorText: result.editorText,
				});
			}

			case "reload": {
				await session.reload();
				return this.success(id, "reload");
			}

			case "export_jsonl": {
				return this.success(id, "export_jsonl", { path: session.exportToJsonl(command.outputPath) });
			}

			case "abort_compaction": {
				session.abortCompaction();
				return this.success(id, "abort_compaction");
			}

			case "abort_branch_summary": {
				session.abortBranchSummary();
				return this.success(id, "abort_branch_summary");
			}

			case "clear_queue": {
				return this.success(id, "clear_queue", session.clearQueue());
			}

			case "get_auth_status": {
				const modelRuntime = session.modelRuntime;
				const oauthProviders = modelRuntime
					.getProviders()
					.filter((provider) => modelRuntime.isUsingOAuth(provider.id))
					.map((provider) => provider.id);
				return this.success(id, "get_auth_status", { oauthProviders: [...oauthProviders] });
			}

			case "refresh_models": {
				await session.modelRuntime.refresh();
				return this.success(id, "refresh_models");
			}

			case "rename_session": {
				const manager = SessionManager.open(command.sessionPath);
				manager.appendSessionInfo(command.name);
				return this.success(id, "rename_session");
			}

			// =================================================================
			// Lifecycle
			// =================================================================

			case "shutdown": {
				connection.send(this.success(id, "shutdown"));
				await connection.flush?.();
				await this.initiateShutdown(0);
				return undefined;
			}

			case "detach": {
				this.explicitlyDetached = true;
				connection.send(this.success(id, "detach"));
				await connection.flush?.();
				connection.close();
				return undefined;
			}

			// =================================================================
			// Filesystem
			// =================================================================

			case "fs_complete": {
				const cwd = session.sessionManager.getCwd();
				const entries = await completePaths(cwd, command.prefix, command.limit);
				return this.success(id, "fs_complete", { entries });
			}

			case "read_file": {
				const cwd = session.sessionManager.getCwd();
				const result = await readSessionFile(cwd, command.path);
				return this.success(id, "read_file", result);
			}

			// =================================================================
			// Sessions
			// =================================================================

			case "list_sessions": {
				const cwd = session.sessionManager.getCwd();
				const sessions = command.all
					? await SessionManager.listAll()
					: await SessionManager.list(cwd, session.sessionManager.getSessionDir());
				return this.success(id, "list_sessions", { sessions: sessions.map(serializeSessionInfo) });
			}

			default: {
				const unknownCommand = command as { type: string };
				return this.error(id, unknownCommand.type, `Unknown command: ${unknownCommand.type}`);
			}
		}
	}

	// =========================================================================
	// Shutdown
	// =========================================================================

	private async checkShutdownRequested(): Promise<void> {
		if (!this.shutdownRequested) return;
		await this.initiateShutdown(0);
	}

	/** Terminate the server: notify client, dispose runtime, invoke onShutdown. */
	async shutdown(exitCode = 0): Promise<void> {
		await this.initiateShutdown(exitCode);
	}

	private async initiateShutdown(exitCode: number): Promise<void> {
		if (this.shuttingDown) return;
		this.shuttingDown = true;

		const connection = this.connection;
		if (connection) {
			this.connection = null;
			try {
				connection.send({ type: "detached", reason: "shutdown" });
				await connection.flush?.();
			} catch {
				// Client already gone.
			}
			connection.close();
		}

		this.unsubscribe?.();
		this.unsubscribeBackpressure?.();
		this.unsubscribe = undefined;
		this.unsubscribeBackpressure = undefined;

		await this.runtimeHost.dispose();
		await this.options.onShutdown(exitCode);
	}
}

function serializeSessionInfo(info: SessionInfo): RpcSessionInfo {
	return {
		path: info.path,
		id: info.id,
		cwd: info.cwd,
		name: info.name,
		parentSessionPath: info.parentSessionPath,
		created: info.created.toISOString(),
		modified: info.modified.toISOString(),
		messageCount: info.messageCount,
		firstMessage: info.firstMessage,
	};
}
