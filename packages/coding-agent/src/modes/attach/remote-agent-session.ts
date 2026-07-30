/**
 * RemoteAgentSession — a facade over RpcClient implementing the AgentSession
 * surface that the stock interactive TUI touches.
 *
 * The TUI binds to a concrete AgentSession, but only structurally: a fixed
 * set of ~60 members. This facade satisfies that surface so the stock TUI
 * can drive an agent running elsewhere (socket or spawned transport) with
 * no behavioral changes to the TUI itself.
 *
 * State is a live mirror: prefetched at attach, then maintained from the
 * event stream. Mutating commands update the mirror synchronously from
 * their responses. `session_changed` triggers a full refetch (driven by
 * RemoteAgentSessionRuntime).
 */

import type { AgentMessage, AgentState, ThinkingLevel } from "@earendil-works/pi-agent-core";
import { contentText, type ImageContent, type Model } from "@earendil-works/pi-ai";
import type { AgentSessionEvent, ExtensionBindings, ModelCycleResult, SessionStats } from "../../core/agent-session.ts";
import type { BashResult } from "../../core/bash-executor.ts";
import type { CompactionResult } from "../../core/compaction/index.ts";
import type { ContextUsage, ToolInfo } from "../../core/extensions/types.ts";
import type { PromptTemplate } from "../../core/prompt-templates.ts";
import { buildContextEntries, type SessionEntry, type SessionTreeNode } from "../../core/session-manager.ts";
import type { SettingsManager } from "../../core/settings-manager.ts";
import type { Skill } from "../../core/skills.ts";
import type { SourceInfo } from "../../core/source-info.ts";
import { addUsageToTotals, createUsageTotals } from "../../core/usage-totals.ts";
import type { Theme } from "../interactive/theme/theme.ts";
import type { ModelInfo, RpcClient, RpcServerEvent } from "../rpc/rpc-client.ts";
import type { RpcExtensionUIRequest, RpcResources, RpcSlashCommand } from "../rpc/rpc-types.ts";

export interface RemoteAgentSessionOptions {
	client: RpcClient;
	/** Client-local settings (theme, keybindings, editor prefs). */
	settingsManager: SettingsManager;
}

interface MirrorState {
	model?: Model<any>;
	thinkingLevel: ThinkingLevel;
	isStreaming: boolean;
	isCompacting: boolean;
	steeringMode: "all" | "one-at-a-time";
	followUpMode: "all" | "one-at-a-time";
	scopedModels: Array<{ model: Model<any>; thinkingLevel?: ThinkingLevel }>;
	sessionFile?: string;
	sessionId: string;
	sessionName?: string;
	autoCompactionEnabled: boolean;
	pendingMessageCount: number;
	retryAttempt: number;
	isBashRunning: boolean;
}

export class RemoteAgentSession {
	private readonly client: RpcClient;
	private readonly localSettingsManager: SettingsManager;
	private _cwd: string;

	private mirror: MirrorState;
	private _messages: AgentMessage[] = [];
	private _entries: SessionEntry[] = [];
	private _leafId: string | null = null;
	private _systemPrompt = "";
	private _contextUsage: ContextUsage | null = null;
	private _tools: ToolInfo[] = [];
	private _availableThinkingLevels: ThinkingLevel[] = ["off"];
	// RpcClient declares getAvailableModels as ModelInfo[], but that is a
	// compile-time projection only: the wire JSON carries full Model<any>
	// objects. We store the declared shape and cast at the TUI boundary.
	private _availableModels: ModelInfo[] = [];
	private _oauthProviders = new Set<string>();
	private _authProviders: Array<{ id: string; name: string; oauth: boolean; apiKey: boolean }> = [];
	private _credentials: Array<{ providerId: string; type: string }> = [];
	private _steeringMessages: string[] = [];
	private _followUpMessages: string[] = [];
	private _resources: RpcResources | undefined;
	private _commands: RpcSlashCommand[] = [];

	private readonly listeners = new Set<(event: AgentSessionEvent) => void>();
	private bindings: ExtensionBindings | undefined;
	private readonly abortController = new AbortController();
	private readonly idleWaiters = new Set<() => void>();
	private readonly bashChunkHandlers = new Map<string, (chunk: string) => void>();

	/** Set by RemoteAgentSessionRuntime to drive refetch + rebind. */
	onSessionChanged: (() => void) | undefined;
	/** Set by the attach CLI to surface server-initiated detach. */
	onDetached: ((reason: string) => void) | undefined;

	private constructor(options: RemoteAgentSessionOptions) {
		this.client = options.client;
		this.localSettingsManager = options.settingsManager;
		const hello = options.client.getHello();
		if (!hello) throw new Error("RpcClient not started — no hello received");
		this._cwd = hello.cwd;
		this.mirror = {
			thinkingLevel: "off",
			isStreaming: false,
			isCompacting: false,
			steeringMode: "all",
			followUpMode: "one-at-a-time",
			scopedModels: [],
			sessionId: hello.sessionId,
			autoCompactionEnabled: true,
			pendingMessageCount: 0,
			retryAttempt: 0,
			isBashRunning: false,
		};
	}

	static async connect(options: RemoteAgentSessionOptions): Promise<RemoteAgentSession> {
		const session = new RemoteAgentSession(options);
		// Subscribe before the initial refetch: events arriving during the
		// refetch window are queued and applied after it (see refetchAll).
		options.client.onEvent((event) => session.routeEvent(event));
		await session.refetchAll();
		return session;
	}

	/** Refresh auth + model mirrors after a login/logout changed them agent-side. */
	private async refreshAuthMirror(): Promise<void> {
		const [auth, models] = await Promise.all([
			this.client.getAuthStatus(),
			this.client.getAvailableModels().catch(() => this._availableModels),
		]);
		this._oauthProviders = new Set(auth.oauthProviders);
		this._authProviders = auth.providers ?? [];
		this._credentials = auth.credentials ?? [];
		this._availableModels = models;
	}

	/** Full mirror refetch. Called at attach and on session_changed. */
	async refetchAll(): Promise<void> {
		this._refetching++;
		try {
			await this.refetchAllInner();
		} finally {
			this._refetching--;
			if (this._refetching === 0) {
				const queued = this._pendingEvents;
				this._pendingEvents = [];
				for (const event of queued) {
					this.routeEvent(event);
				}
			}
		}
	}

	private async refetchAllInner(): Promise<void> {
		const [state, entries, messages, systemPrompt, usage, tools, levels, models, auth, resources, commands] =
			await Promise.all([
				this.client.getState(),
				this.client.getEntries(),
				this.client.getMessages(),
				this.client.getSystemPrompt(),
				this.client.getContextUsage(),
				this.client.getTools(),
				this.client.getAvailableThinkingLevels(),
				this.client.getAvailableModels(),
				this.client.getAuthStatus(),
				this.client.getResources(),
				this.client.getCommands(),
			]);

		this.mirror = {
			...this.mirror,
			model: state.model ?? undefined,
			thinkingLevel: state.thinkingLevel,
			isStreaming: state.isStreaming,
			isCompacting: state.isCompacting,
			steeringMode: state.steeringMode,
			followUpMode: state.followUpMode,
			scopedModels: state.scopedModels,
			sessionFile: state.sessionFile,
			sessionId: state.sessionId,
			sessionName: state.sessionName,
			autoCompactionEnabled: state.autoCompactionEnabled,
			pendingMessageCount: state.pendingMessageCount,
		};
		this._entries = entries.entries;
		this._leafId = entries.leafId;
		// After a reconnect the hello (and its cwd) belongs to the new agent;
		// during a session_changed rebind the hello is stale, so only adopt it
		// when it matches the session we just fetched.
		const hello = this.client.getHello();
		if (hello && hello.sessionId === state.sessionId) {
			this._cwd = hello.cwd;
		}
		this._messages = messages;
		this._systemPrompt = systemPrompt;
		this._contextUsage = usage;
		this._tools = tools;
		this._availableThinkingLevels = levels;
		this._availableModels = models;
		this._oauthProviders = new Set(auth.oauthProviders);
		this._authProviders = auth.providers ?? [];
		this._credentials = auth.credentials ?? [];
		this._commands = commands;
		this._resources = resources;
	}

	// =========================================================================
	// Event routing and mirror maintenance
	// =========================================================================

	/** >0 while a refetch is in flight; events queue instead of applying. */
	private _refetching = 0;
	/** Client-side auth callbacks for in-flight login flows, keyed by request id. */
	private _authInteractions = new Map<
		string,
		{
			prompt: (prompt: {
				type: "text" | "secret" | "select" | "manual_code";
				message: string;
				placeholder?: string;
				options?: readonly { id: string; label: string; description?: string }[];
			}) => Promise<string>;
			notify: (event: never) => void;
		}
	>();
	private _authRequestCounter = 0;
	private _pendingEvents: RpcServerEvent[] = [];

	private routeEvent(event: RpcServerEvent): void {
		if (this._refetching > 0) {
			// The refetch replaces mirrored state wholesale; applying events
			// mid-flight would interleave stale and fresh data. Queue and drain.
			this._pendingEvents.push(event);
			return;
		}
		switch (event.type) {
			case "auth_prompt": {
				const authEvent = event as {
					requestId: string;
					prompt: {
						kind: "text" | "secret" | "select" | "manual_code";
						message: string;
						placeholder?: string;
						options?: Array<{ id: string; label: string; description?: string }>;
					};
				};
				const interaction = this._authInteractions.get(authEvent.requestId);
				if (!interaction) return;
				const { kind, ...rest } = authEvent.prompt;
				interaction
					.prompt({ type: kind, ...rest })
					.then((value) => this.client.respondAuthPrompt(authEvent.requestId, { value }))
					.catch(() => this.client.respondAuthPrompt(authEvent.requestId, { cancelled: true }));
				return;
			}
			case "auth_notify": {
				const authEvent = event as { requestId: string; event: unknown };
				const interaction = this._authInteractions.get(authEvent.requestId);
				interaction?.notify(authEvent.event as Parameters<typeof interaction.notify>[0]);
				return;
			}
			case "extension_ui_request":
				void this.handleUIRequest(event as RpcExtensionUIRequest);
				return;
			case "session_changed": {
				const changed = event as { sessionId?: string; cwd?: string };
				if (changed.sessionId) this.mirror.sessionId = changed.sessionId;
				if (changed.cwd) this._cwd = changed.cwd;
				this.onSessionChanged?.();
				return;
			}
			case "detached":
				this.onDetached?.((event as { reason?: string }).reason ?? "unknown");
				return;
			case "extension_error": {
				const err = event as { extensionPath?: string; event?: string; error?: string };
				this.bindings?.onError?.({
					extensionPath: err.extensionPath ?? "unknown",
					event: err.event ?? "unknown",
					error: err.error ?? "unknown",
				});
				return;
			}
			case "bash_execution_update": {
				const update = event as { id?: string; delta?: string };
				if (update.id && update.delta) this.bashChunkHandlers.get(update.id)?.(update.delta);
				break;
			}
		}
		this.applyMirror(event as AgentSessionEvent);
		this.emit(event as AgentSessionEvent);
	}

	private applyMirror(event: AgentSessionEvent): void {
		const e = event as unknown as Record<string, unknown>;
		switch (event.type) {
			case "agent_start":
				this.mirror.isStreaming = true;
				break;
			case "agent_end":
				this.mirror.isStreaming = false;
				void this.client
					.getContextUsage()
					.then((usage) => {
						this._contextUsage = usage;
					})
					.catch(() => {});
				break;
			case "agent_settled":
				for (const waiter of [...this.idleWaiters]) waiter();
				break;
			case "message_end":
				if (e.message) {
					const message = e.message as AgentMessage;
					// Same replay hazard as entry_appended below: a message that
					// completed while a refetch was in flight is already in the
					// fresh snapshot — replaying its queued message_end would
					// double-render it. Messages carry ms timestamps, so
					// role+timestamp identity is sufficient here.
					const last = this._messages[this._messages.length - 1] as
						| { role?: string; timestamp?: number }
						| undefined;
					const incoming = message as { role?: string; timestamp?: number };
					if (!last || last.role !== incoming.role || last.timestamp !== incoming.timestamp) {
						this._messages.push(message);
					}
				}
				break;
			case "queue_update":
				this._steeringMessages = (e.steering as string[]) ?? [];
				this._followUpMessages = (e.followUp as string[]) ?? [];
				this.mirror.pendingMessageCount = this._steeringMessages.length + this._followUpMessages.length;
				break;
			case "thinking_level_changed":
				this.mirror.thinkingLevel = e.level as ThinkingLevel;
				break;
			case "compaction_start":
				this.mirror.isCompacting = true;
				break;
			case "compaction_end":
				this.mirror.isCompacting = false;
				break;
			case "auto_retry_start":
				this.mirror.retryAttempt = e.attempt as number;
				break;
			case "auto_retry_end":
				if (e.success) this.mirror.retryAttempt = 0;
				break;
			case "entry_appended": {
				const entry = e.entry as SessionEntry;
				// Dedup: a queued event drained after a refetch may replay an
				// entry the refetched snapshot already contains.
				if (this._entries.some((existing) => existing.id === entry.id)) break;
				this._entries.push(entry);
				this._leafId = entry.id;
				break;
			}
			case "session_info_changed":
				this.mirror.sessionName = e.name as string | undefined;
				break;
		}
	}

	private emit(event: AgentSessionEvent): void {
		for (const listener of [...this.listeners]) {
			try {
				listener(event);
			} catch {
				// Listener errors must not break the mirror.
			}
		}
	}

	// =========================================================================
	// Extension UI bridge
	// =========================================================================

	private async handleUIRequest(request: RpcExtensionUIRequest): Promise<void> {
		const ui = this.bindings?.uiContext;
		const respond = (response: object): void => {
			this.client.respondToExtensionUI({ type: "extension_ui_response", id: request.id, ...response } as never);
		};
		if (!ui) {
			respond({ cancelled: true });
			return;
		}
		try {
			switch (request.method) {
				case "select": {
					const value = await ui.select(request.title, request.options, { timeout: request.timeout });
					respond(value === undefined ? { cancelled: true } : { value });
					return;
				}
				case "confirm": {
					const confirmed = await ui.confirm(request.title, request.message, { timeout: request.timeout });
					respond({ confirmed });
					return;
				}
				case "input": {
					const value = await ui.input(request.title, request.placeholder, { timeout: request.timeout });
					respond(value === undefined ? { cancelled: true } : { value });
					return;
				}
				case "editor": {
					const value = await ui.editor?.(request.title, request.prefill);
					respond(value === undefined ? { cancelled: true } : { value });
					return;
				}
				// Fire-and-forget methods: no response expected.
				case "notify":
					ui.notify(request.message, request.notifyType);
					return;
				case "setStatus":
					ui.setStatus(request.statusKey, request.statusText);
					return;
				case "setWidget":
					ui.setWidget(request.widgetKey, request.widgetLines, { placement: request.widgetPlacement });
					return;
				case "setTitle":
					ui.setTitle?.(request.title);
					return;
				case "set_editor_text":
					ui.setEditorText?.(request.text);
					return;
				default:
					// Unknown method (newer agent than this client): settle the
					// server-side dialog instead of leaving it pending.
					respond({ cancelled: true });
					return;
			}
		} catch {
			respond({ cancelled: true });
		}
	}

	// =========================================================================
	// AgentSession surface — subscriptions and bindings
	// =========================================================================

	subscribe(listener: (event: AgentSessionEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	async bindExtensions(bindings: ExtensionBindings): Promise<void> {
		this.bindings = bindings;
	}

	// =========================================================================
	// AgentSession surface — prompting
	// =========================================================================

	async prompt(text: string, options?: { images?: ImageContent[]; streamingBehavior?: "steer" | "followUp" }) {
		if (this.mirror.isStreaming) {
			if (options?.streamingBehavior === "steer") {
				await this.client.steer(text, options?.images);
			} else {
				await this.client.followUp(text, options?.images);
			}
			return;
		}
		await this.client.prompt(text, options?.images);
	}

	async steer(message: string, images?: ImageContent[]): Promise<void> {
		await this.client.steer(message, images);
	}

	async followUp(message: string, images?: ImageContent[]): Promise<void> {
		await this.client.followUp(message, images);
	}

	async abort(): Promise<void> {
		await this.client.abort();
		this.bindings?.abortHandler?.();
	}

	async abortCompaction(): Promise<void> {
		await this.client.abortCompaction();
	}

	async abortBranchSummary(): Promise<void> {
		await this.client.abortBranchSummary();
	}

	async abortRetry(): Promise<void> {
		await this.client.abortRetry();
	}

	async abortBash(): Promise<void> {
		await this.client.abortBash();
		this.mirror.isBashRunning = false;
	}

	waitForIdle(): Promise<void> {
		if (!this.mirror.isStreaming && !this.mirror.isBashRunning) return Promise.resolve();
		return new Promise((resolve) => {
			const waiter = () => {
				this.idleWaiters.delete(waiter);
				resolve();
			};
			this.idleWaiters.add(waiter);
			// agent_settled never fires while only a local bash runs; poll the
			// mirror so this still resolves when executeBash finishes.
			if (!this.mirror.isStreaming && this.mirror.isBashRunning) {
				const poll = setInterval(() => {
					if (!this.mirror.isStreaming && !this.mirror.isBashRunning) {
						clearInterval(poll);
						waiter();
					}
				}, 50);
				if (typeof poll.unref === "function") poll.unref();
			}
		});
	}

	// =========================================================================
	// AgentSession surface — model
	// =========================================================================

	async setModel(model: Model<any>): Promise<void> {
		// The full model comes from the TUI (selected out of getAvailable);
		// the RPC response only confirms the switch.
		await this.client.setModel(model.provider, model.id);
		this.mirror.model = model;
		this._availableThinkingLevels = await this.client
			.getAvailableThinkingLevels()
			.catch(() => this._availableThinkingLevels);
	}

	async cycleModel(): Promise<ModelCycleResult | null> {
		const result = await this.client.cycleModel();
		if (!result) return null;
		// The cycle response only carries {provider, id}; refetch state for
		// the full model object.
		const state = await this.client.getState();
		this.mirror.model = state.model ?? undefined;
		this.mirror.thinkingLevel = result.thinkingLevel;
		if (!this.mirror.model) return null;
		return { model: this.mirror.model, thinkingLevel: result.thinkingLevel, isScoped: result.isScoped };
	}

	setThinkingLevel(level: ThinkingLevel): void {
		this.mirror.thinkingLevel = level;
		void this.client.setThinkingLevel(level).catch(() => {});
	}

	async cycleThinkingLevel(): Promise<ThinkingLevel | null> {
		const result = await this.client.cycleThinkingLevel();
		if (result) this.mirror.thinkingLevel = result.level;
		return result?.level ?? null;
	}

	getAvailableThinkingLevels(): ThinkingLevel[] {
		return this._availableThinkingLevels;
	}

	// =========================================================================
	// AgentSession surface — queues and modes
	// =========================================================================

	getSteeringMessages(): string[] {
		return this._steeringMessages;
	}

	getFollowUpMessages(): string[] {
		return this._followUpMessages;
	}

	// Synchronous like AgentSession.clearQueue (the TUI destructures the
	// result without awaiting): clear the mirror immediately and let the
	// server confirm via queue_update.
	clearQueue(): { steering: string[]; followUp: string[] } {
		const cleared = { steering: this._steeringMessages, followUp: this._followUpMessages };
		this._steeringMessages = [];
		this._followUpMessages = [];
		this.mirror.pendingMessageCount = 0;
		void this.client.clearQueue().catch(() => {});
		return cleared;
	}

	setSteeringMode(mode: "all" | "one-at-a-time"): void {
		this.mirror.steeringMode = mode;
		void this.client.setSteeringMode(mode).catch(() => {});
	}

	setFollowUpMode(mode: "all" | "one-at-a-time"): void {
		this.mirror.followUpMode = mode;
		void this.client.setFollowUpMode(mode).catch(() => {});
	}

	setAutoCompactionEnabled(enabled: boolean): void {
		this.mirror.autoCompactionEnabled = enabled;
		void this.client.setAutoCompaction(enabled).catch(() => {});
	}

	setAutoRetryEnabled(enabled: boolean): void {
		void this.client.setAutoRetry(enabled).catch(() => {});
	}

	async setScopedModels(models: Array<{ model: Model<any>; thinkingLevel?: ThinkingLevel }>): Promise<void> {
		await this.client.setScopedModels(
			models.map((entry) => ({
				provider: entry.model.provider,
				id: entry.model.id,
				thinkingLevel: entry.thinkingLevel,
			})),
		);
		this.mirror.scopedModels = models;
	}

	// =========================================================================
	// AgentSession surface — compaction, tree, stats
	// =========================================================================

	async compact(customInstructions?: string): Promise<CompactionResult> {
		return this.client.compact(customInstructions);
	}

	async navigateTree(
		targetId: string,
		options: { summarize?: boolean; customInstructions?: string; replaceInstructions?: boolean; label?: string } = {},
	): Promise<{ editorText?: string; cancelled: boolean }> {
		const result = await this.client.navigateTree(targetId, options);
		await this.refetchEntries();
		return result;
	}

	// Mirrors AgentSession.getSessionStats, computed client-side over the
	// mirrored entries: the TUI calls this synchronously.
	getSessionStats(): SessionStats {
		let userMessages = 0;
		let assistantMessages = 0;
		let toolResults = 0;
		let totalMessages = 0;
		let toolCalls = 0;
		const usageTotals = createUsageTotals();

		for (const entry of this._entries) {
			if ((entry.type === "branch_summary" || entry.type === "compaction") && entry.usage) {
				addUsageToTotals(usageTotals, entry.usage);
			}
			if (entry.type !== "message") continue;
			totalMessages++;
			const message = entry.message;
			if (message.role === "user") {
				userMessages++;
			} else if (message.role === "toolResult") {
				toolResults++;
				if (message.usage) {
					addUsageToTotals(usageTotals, message.usage);
				}
			} else if (message.role === "assistant") {
				assistantMessages++;
				if (Array.isArray(message.content)) {
					toolCalls += message.content.filter((c) => c.type === "toolCall").length;
				}
				addUsageToTotals(usageTotals, message.usage);
			}
		}

		return {
			sessionFile: this.mirror.sessionFile,
			sessionId: this.mirror.sessionId,
			userMessages,
			assistantMessages,
			toolCalls,
			toolResults,
			totalMessages,
			tokens: {
				input: usageTotals.input,
				output: usageTotals.output,
				cacheRead: usageTotals.cacheRead,
				cacheWrite: usageTotals.cacheWrite,
				total: usageTotals.input + usageTotals.output + usageTotals.cacheRead + usageTotals.cacheWrite,
			},
			cost: usageTotals.cost,
			contextUsage: this.getContextUsage(),
		};
	}

	getContextUsage(): ContextUsage | undefined {
		return this._contextUsage ?? undefined;
	}

	async exportToHtml(outputPath?: string): Promise<string> {
		return (await this.client.exportHtml(outputPath)).path;
	}

	exportToJsonl(_outputPath?: string): string {
		throw new Error("exportToJsonl is async in attach mode — use exportToJsonlAsync");
	}

	async exportToJsonlAsync(outputPath?: string): Promise<string> {
		return (await this.client.exportJsonl(outputPath)).path;
	}

	async reload(): Promise<void> {
		await this.client.reload();
		await this.refetchAll();
	}

	getLastAssistantText(): string | undefined {
		for (let i = this._messages.length - 1; i >= 0; i--) {
			const message = this._messages[i];
			if (message.role === "assistant") {
				const text = (message.content as Array<{ type: string; text?: string }>)
					.filter((part) => part.type === "text")
					.map((part) => part.text ?? "")
					.join("");
				if (text) return text;
			}
		}
		return undefined;
	}

	// Mirrors AgentSession.getUserMessagesForForking, computed client-side
	// over the mirrored entries: the TUI consumes this synchronously.
	getUserMessagesForForking(): Array<{ entryId: string; text: string }> {
		const result: Array<{ entryId: string; text: string }> = [];
		for (const entry of this._entries) {
			if (entry.type !== "message") continue;
			if (entry.message.role !== "user") continue;
			const text = contentText(entry.message.content, "");
			if (text) {
				result.push({ entryId: entry.id, text });
			}
		}
		return result;
	}

	// =========================================================================
	// AgentSession surface — bash
	// =========================================================================

	async executeBash(
		command: string,
		onChunk?: (chunk: string) => void,
		options?: { excludeFromContext?: boolean },
	): Promise<BashResult> {
		const requestId = `bash-${Date.now()}-${Math.random().toString(36).slice(2)}`;
		if (onChunk) this.bashChunkHandlers.set(requestId, onChunk);
		this.mirror.isBashRunning = true;
		try {
			return await this.client.bashWithId(requestId, command, options?.excludeFromContext);
		} finally {
			this.bashChunkHandlers.delete(requestId);
			this.mirror.isBashRunning = false;
		}
	}

	/** Server records bash results; the entry_appended events mirror them back. */
	recordBashResult(_command: string, _result: BashResult, _options?: { excludeFromContext?: boolean }): void {}

	// =========================================================================
	// AgentSession surface — tools
	// =========================================================================

	getToolDefinition(toolName: string): ToolInfo | undefined {
		return this._tools.find((tool) => tool.name === toolName);
	}

	getAllTools(): ToolInfo[] {
		return this._tools;
	}

	// =========================================================================
	// AgentSession surface — getters
	// =========================================================================

	get model(): Model<any> | undefined {
		return this.mirror.model;
	}
	get thinkingLevel(): ThinkingLevel {
		return this.mirror.thinkingLevel;
	}
	get isStreaming(): boolean {
		return this.mirror.isStreaming;
	}
	get isCompacting(): boolean {
		return this.mirror.isCompacting;
	}
	get isIdle(): boolean {
		return !this.mirror.isStreaming && !this.mirror.isBashRunning;
	}
	get isBashRunning(): boolean {
		return this.mirror.isBashRunning;
	}
	get retryAttempt(): number {
		return this.mirror.retryAttempt;
	}
	get autoCompactionEnabled(): boolean {
		return this.mirror.autoCompactionEnabled;
	}
	get steeringMode(): "all" | "one-at-a-time" {
		return this.mirror.steeringMode;
	}
	get followUpMode(): "all" | "one-at-a-time" {
		return this.mirror.followUpMode;
	}
	get scopedModels(): ReadonlyArray<{ model: Model<any>; thinkingLevel?: ThinkingLevel }> {
		return this.mirror.scopedModels;
	}
	get pendingMessageCount(): number {
		return this.mirror.pendingMessageCount;
	}
	get sessionFile(): string | undefined {
		return this.mirror.sessionFile;
	}
	get sessionId(): string {
		return this.mirror.sessionId;
	}
	get sessionName(): string | undefined {
		return this.mirror.sessionName;
	}
	get systemPrompt(): string {
		return this._systemPrompt;
	}
	get messages(): AgentMessage[] {
		return this._messages;
	}
	get state(): AgentState {
		return {
			systemPrompt: this._systemPrompt,
			model: this.mirror.model,
			thinkingLevel: this.mirror.thinkingLevel,
			tools: [],
			messages: this._messages,
			isStreaming: this.mirror.isStreaming,
		} as unknown as AgentState;
	}
	get promptTemplates(): Array<Omit<PromptTemplate, "content">> {
		return this._resources?.prompts ?? [];
	}
	get settingsManager(): SettingsManager {
		return this.localSettingsManager;
	}

	async setSessionName(name: string): Promise<void> {
		await this.client.setSessionName(name);
		this.mirror.sessionName = name;
	}

	private async refetchEntries(): Promise<void> {
		const result = await this.client.getEntries().catch(() => undefined);
		if (result) {
			this._entries = result.entries;
			this._leafId = result.leafId;
		}
	}

	dispose(): void {
		this.abortController.abort();
		this.listeners.clear();
		this.idleWaiters.clear();
		this.bashChunkHandlers.clear();
	}

	// =========================================================================
	// Sub-facades
	// =========================================================================

	readonly modelRuntime = {
		getAvailable: async (): Promise<Model<any>[]> => this._availableModels as unknown as Model<any>[],
		getAvailableSnapshot: (): Model<any>[] => this._availableModels as unknown as Model<any>[],
		refresh: async (_options?: {
			signal?: AbortSignal;
		}): Promise<{ aborted: boolean; errors: Map<string, Error> }> => {
			// The wire response carries no per-provider error detail (remote
			// refresh failures surface agent-side), and a client-side abort is
			// not forwarded — the server refresh completes regardless.
			await this.client.refreshModels();
			this._availableModels = await this.client.getAvailableModels().catch(() => this._availableModels);
			return { aborted: false, errors: new Map() };
		},
		isUsingOAuth: (provider: string): boolean => this._oauthProviders.has(provider),
		getModel: (provider: string, modelId: string): Model<any> | undefined =>
			this._availableModels.find((m) => m.provider === provider && m.id === modelId) as unknown as
				| Model<any>
				| undefined,
		// Provider capabilities mirrored from the agent's get_auth_status, so
		// /login lists the agent's real providers; the login/logout actions
		// themselves stay non-remotable and fail with a friendly error box.
		getProviders: (): Array<{ id: string; name: string; auth: { oauth?: boolean; apiKey?: boolean } }> =>
			this._authProviders.map((p) => ({
				id: p.id,
				name: p.name,
				auth: { oauth: p.oauth || undefined, apiKey: p.apiKey || undefined },
			})),
		getProvider: (
			provider: string,
		): { id: string; name: string; auth: { oauth?: boolean; apiKey?: boolean } } | undefined => {
			const found = this._authProviders.find((p) => p.id === provider);
			return found
				? {
						id: found.id,
						name: found.name,
						auth: { oauth: found.oauth || undefined, apiKey: found.apiKey || undefined },
					}
				: undefined;
		},
		getError: (): string | undefined => undefined,
		getProviderAuthStatus: (provider: string): { configured: boolean } => ({
			configured: this._oauthProviders.has(provider),
		}),
		getAuth: (provider: string): { provider: string; oauth: boolean } | undefined =>
			this._oauthProviders.has(provider) ? { provider, oauth: true } : undefined,
		checkAuth: async (provider?: string): Promise<boolean> =>
			provider ? this._oauthProviders.has(provider) : this._oauthProviders.size > 0,
		// Bridged over the wire: the provider's auth flow runs on the agent
		// host; its prompt/notify callbacks arrive as auth_prompt/auth_notify
		// events and are served by the TUI's own dialogs via the interaction
		// object the TUI passes in here.
		login: async (
			provider: string,
			method: "api_key" | "oauth",
			interaction: {
				prompt: (prompt: {
					type: "text" | "secret" | "select" | "manual_code";
					message: string;
					placeholder?: string;
					options?: readonly { id: string; label: string; description?: string }[];
				}) => Promise<string>;
				notify: (event: never) => void;
			},
		): Promise<void> => {
			const requestId = `login_${++this._authRequestCounter}`;
			this._authInteractions.set(requestId, interaction);
			try {
				await this.client.loginWithId(requestId, provider, method);
				await this.refreshAuthMirror();
			} finally {
				this._authInteractions.delete(requestId);
			}
		},
		logout: async (provider: string): Promise<void> => {
			await this.client.logout(provider);
			await this.refreshAuthMirror();
		},
		// Rendering member: the logout selector lists the agent's stored
		// credentials (mirrored via get_auth_status). The logout action itself
		// still throws a friendly error, which the TUI surfaces in an error box.
		listCredentials: async (): Promise<Array<{ providerId: string; type: string }>> => this._credentials,
	};

	readonly extensionRunner = {
		// Extension commands execute agent-side via session.prompt(); the TUI
		// only needs names/metadata for autocomplete and queueing decisions.
		getRegisteredCommands: (): Array<{
			name: string;
			invocationName: string;
			description?: string;
			sourceInfo: SourceInfo;
			getArgumentCompletions?: undefined;
		}> =>
			this._commands
				.filter((c) => c.source === "extension")
				.map((c) => ({
					name: c.name,
					invocationName: c.name,
					description: c.description,
					sourceInfo: c.sourceInfo,
					getArgumentCompletions: undefined,
				})),
		getCommand: (name: string): { name: string; invocationName: string } | undefined => {
			const found = this._commands.find((c) => c.source === "extension" && c.name === name);
			return found ? { name: found.name, invocationName: found.name } : undefined;
		},
		// user_bash is emitted agent-side by the RPC bash handler (which also
		// honours interception results), so the local pre-emit is a no-op.
		emitUserBash: async (): Promise<undefined> => undefined,
		getMessageRenderer: (): undefined => undefined,
		getEntryRenderer: (): undefined => undefined,
		getShortcuts: (): unknown[] => [],
		getCommandDiagnostics: (): unknown[] => [],
		getShortcutDiagnostics: (): unknown[] => [],
		getModelRegistry: (): unknown => this.modelRuntime,
	};

	readonly resourceLoader = {
		getSkills: (): { skills: Skill[]; diagnostics: unknown[] } => ({
			skills: this._resources?.skills ?? [],
			diagnostics: [],
		}),
		getPrompts: (): {
			prompts: Array<Omit<PromptTemplate, "content"> & { content: string }>;
			diagnostics: unknown[];
		} => ({
			prompts: (this._resources?.prompts ?? []).map((p) => ({ ...p, content: "" })),
			diagnostics: [],
		}),
		getThemes: (): { themes: Theme[]; diagnostics: unknown[] } => ({
			themes: this._resources?.themes ?? [],
			diagnostics: [],
		}),
		getExtensions: (): {
			extensions: unknown[];
			errors: Array<{ path: string; error: string }>;
			runtime: unknown;
		} => ({
			extensions: this._resources?.extensions ?? [],
			errors: this._resources?.extensionErrors ?? [],
			runtime: {},
		}),
		getAgentsFiles: (): { agentsFiles: Array<{ path: string }> } => ({
			agentsFiles: this._resources?.agentsFiles ?? [],
		}),
		getSystemPromptSource: (): { path: string } | undefined => this._resources?.systemPromptSource,
		getAppendSystemPromptSources: (): Array<{ path: string }> => this._resources?.appendSystemPromptSources ?? [],
		extendResources: (): void => {},
		reload: async (): Promise<void> => {
			await this.client.reload();
			this._resources = await this.client.getResources().catch(() => this._resources);
			this._commands = await this.client.getCommands().catch(() => this._commands);
		},
	};

	readonly agent: { signal: AbortSignal; transport: unknown; subscribe: () => () => void } = {
		signal: this.abortController.signal,
		transport: undefined as unknown,
		subscribe: (): (() => void) => () => {},
	};

	readonly sessionManager = {
		getCwd: (): string => this._cwd,
		getSessionFile: (): string | undefined => this.mirror.sessionFile,
		getSessionId: (): string => this.mirror.sessionId,
		getSessionName: (): string | undefined => this.mirror.sessionName,
		getSessionDir: (): string | undefined => {
			const file = this.mirror.sessionFile;
			return file ? file.slice(0, file.lastIndexOf("/")) : undefined;
		},
		getEntries: (): SessionEntry[] => this._entries,
		getLeafId: (): string | null => this._leafId,
		// Mirrors SessionManager.getTree, computed client-side over the
		// mirrored entries: the TUI's tree selector consumes this synchronously.
		getTree: (): SessionTreeNode[] => {
			const entries = this._entries;
			const labelsById = new Map<string, string>();
			const labelTimestampsById = new Map<string, string>();
			for (const entry of entries) {
				if (entry.type === "label") {
					if (entry.label) {
						labelsById.set(entry.targetId, entry.label);
						labelTimestampsById.set(entry.targetId, entry.timestamp);
					} else {
						labelsById.delete(entry.targetId);
						labelTimestampsById.delete(entry.targetId);
					}
				}
			}
			const nodeMap = new Map<string, SessionTreeNode>();
			const roots: SessionTreeNode[] = [];
			for (const entry of entries) {
				nodeMap.set(entry.id, {
					entry,
					children: [],
					label: labelsById.get(entry.id),
					labelTimestamp: labelTimestampsById.get(entry.id),
				});
			}
			for (const entry of entries) {
				const node = nodeMap.get(entry.id)!;
				if (entry.parentId === null || entry.parentId === entry.id) {
					roots.push(node);
				} else {
					const parent = nodeMap.get(entry.parentId);
					if (parent) {
						parent.children.push(node);
					} else {
						roots.push(node);
					}
				}
			}
			const stack: SessionTreeNode[] = [...roots];
			while (stack.length > 0) {
				const node = stack.pop()!;
				node.children.sort((a, b) => new Date(a.entry.timestamp).getTime() - new Date(b.entry.timestamp).getTime());
				stack.push(...node.children);
			}
			return roots;
		},
		buildContextEntries: (): SessionEntry[] => buildContextEntries(this._entries, this._leafId),
		usesDefaultSessionDir: (): boolean => true,
		isPersisted: (): boolean => this.mirror.sessionFile !== undefined,
		getHeader: (): undefined => undefined,
		appendLabelChange: (): void => {
			// Tree labels are not remotable in v1.
		},
	};
}
