/**
 * RPC protocol types for headless operation.
 *
 * Commands are sent as JSON lines on stdin.
 * Responses and events are emitted as JSON lines on stdout.
 */

import type { AgentMessage, ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ImageContent, Model } from "@earendil-works/pi-ai";
import type { SessionStats } from "../../core/agent-session.ts";
import type { BashResult } from "../../core/bash-executor.ts";
import type { CompactionResult } from "../../core/compaction/index.ts";
import type { ContextUsage, ToolInfo } from "../../core/extensions/types.ts";
import type { PromptTemplate } from "../../core/prompt-templates.ts";
import type { SessionEntry, SessionTreeNode } from "../../core/session-manager.ts";
import type { Skill } from "../../core/skills.ts";
import type { SourceInfo } from "../../core/source-info.ts";
import type { Theme } from "../interactive/theme/theme.ts";

// ============================================================================
// RPC Commands (stdin)
// ============================================================================

export type RpcCommand =
	// Prompting
	| { id?: string; type: "prompt"; message: string; images?: ImageContent[]; streamingBehavior?: "steer" | "followUp" }
	| { id?: string; type: "steer"; message: string; images?: ImageContent[] }
	| { id?: string; type: "follow_up"; message: string; images?: ImageContent[] }
	| { id?: string; type: "abort" }
	| { id?: string; type: "new_session"; parentSession?: string }

	// State
	| { id?: string; type: "get_state" }

	// Model
	| { id?: string; type: "set_model"; provider: string; modelId: string }
	| { id?: string; type: "cycle_model" }
	| { id?: string; type: "get_available_models" }

	// Thinking
	| { id?: string; type: "set_thinking_level"; level: ThinkingLevel }
	| { id?: string; type: "cycle_thinking_level" }
	| { id?: string; type: "get_available_thinking_levels" }

	// Queue modes
	| { id?: string; type: "set_steering_mode"; mode: "all" | "one-at-a-time" }
	| { id?: string; type: "set_follow_up_mode"; mode: "all" | "one-at-a-time" }

	// Compaction
	| { id?: string; type: "compact"; customInstructions?: string }
	| { id?: string; type: "set_auto_compaction"; enabled: boolean }

	// Retry
	| { id?: string; type: "set_auto_retry"; enabled: boolean }
	| { id?: string; type: "abort_retry" }

	// Bash
	| { id?: string; type: "bash"; command: string; excludeFromContext?: boolean }
	| { id?: string; type: "abort_bash" }

	// Session
	| { id?: string; type: "get_session_stats" }
	| { id?: string; type: "export_html"; outputPath?: string }
	| { id?: string; type: "switch_session"; sessionPath: string; cwdOverride?: string }
	| { id?: string; type: "delete_session"; sessionPath: string }
	| { id?: string; type: "fork"; entryId: string }
	| { id?: string; type: "clone" }
	| { id?: string; type: "get_fork_messages" }
	| { id?: string; type: "get_entries"; since?: string }
	| { id?: string; type: "get_tree" }
	| { id?: string; type: "get_last_assistant_text" }
	| { id?: string; type: "set_session_name"; name: string }

	// Messages
	| { id?: string; type: "get_messages" }

	// Commands (available for invocation via prompt)
	| { id?: string; type: "get_commands" }

	// Lifecycle
	| { id?: string; type: "shutdown" }
	| { id?: string; type: "detach" }

	// Remote-attach additions (P2)
	| { id?: string; type: "get_context_usage" }
	| { id?: string; type: "get_system_prompt" }
	| { id?: string; type: "get_tools" }
	| { id?: string; type: "get_resources" }
	| {
			id?: string;
			type: "set_scoped_models";
			models: Array<{ provider: string; id: string; thinkingLevel?: ThinkingLevel }>;
	  }
	| {
			id?: string;
			type: "navigate_tree";
			targetId: string;
			summarize?: boolean;
			customInstructions?: string;
			replaceInstructions?: boolean;
			label?: string;
	  }
	| { id?: string; type: "reload" }
	| { id?: string; type: "export_jsonl"; outputPath?: string }
	| { id?: string; type: "abort_compaction" }
	| { id?: string; type: "abort_branch_summary" }
	| { id?: string; type: "clear_queue" }
	| { id?: string; type: "get_auth_status" }
	| { id?: string; type: "refresh_models" }
	| { id?: string; type: "login"; provider: string; method: "api_key" | "oauth" }
	| { id?: string; type: "logout"; provider: string }
	/** Response to an auth_prompt event; `requestId` correlates with the login command id. */
	| { id?: string; type: "auth_response"; requestId: string; value?: string; cancelled?: boolean }
	/** Upload a session file (JSONL content) into the agent's session dir and switch to it. */
	| { id?: string; type: "import_session"; content: string; fileName: string; cwdOverride?: string }
	| { id?: string; type: "rename_session"; sessionPath: string; name: string }

	// Filesystem (executed against the agent-side filesystem)
	| { id?: string; type: "fs_complete"; prefix: string; limit?: number }
	| { id?: string; type: "read_file"; path: string }

	// Sessions
	| { id?: string; type: "list_sessions"; all?: boolean };

/** Protocol version emitted in the `hello` greeting. */
export const RPC_PROTOCOL_VERSION = 1;

/** Optional capabilities advertised in `hello` (command types beyond the baseline). */
export const RPC_CAPABILITIES = [
	"shutdown",
	"detach",
	"fs_complete",
	"read_file",
	"list_sessions",
	"login",
	"logout",
	"import_session",
	"delete_session",
] as const;
export type RpcCapability = (typeof RPC_CAPABILITIES)[number];

// ============================================================================
// RPC Slash Command (for get_commands response)
// ============================================================================

/** A command available for invocation via prompt */
/** Auth state of the agent host, mirrored by attach clients. */
export interface RpcAuthStatus {
	/** Providers currently authenticated via OAuth. */
	oauthProviders: string[];
	/**
	 * All providers with their auth capabilities. Method entries mirror the
	 * presence/shape of the agent's real method objects (functions obviously
	 * can't cross the wire): the TUI checks `.login` truthiness and
	 * `"loginLabel" in method`, so booleans here would misroute API-key login
	 * to the ambient-auth dialog (and throw on the `in` check).
	 */
	providers: Array<{
		id: string;
		name: string;
		oauth?: { loginLabel?: string };
		apiKey?: { login: true };
	}>;
	/** Stored credentials (logout selector). */
	credentials: Array<{ providerId: string; type: string }>;
}

export interface RpcSlashCommand {
	/** Command name (without leading slash) */
	name: string;
	/** Human-readable description */
	description?: string;
	/** What kind of command this is */
	source: "extension" | "prompt" | "skill";
	/** Source metadata for the owning resource */
	sourceInfo: SourceInfo;
	/** Argument hint shown in autocomplete, e.g. "<file> [options]". Prompt templates only. */
	argumentHint?: string;
}

// ============================================================================
// RPC Resources (for get_resources response)
// ============================================================================

/**
 * Metadata about the resources loaded into the agent session. Everything
 * a remote UI needs for autocomplete and the "loaded resources" display.
 * Prompt template content is omitted — expansion happens agent-side in
 * session.prompt().
 */
export interface RpcResources {
	skills: Skill[];
	prompts: Array<Omit<PromptTemplate, "content">>;
	themes: Theme[];
	extensions: Array<{ path: string; sourceInfo?: SourceInfo; hidden?: boolean }>;
	extensionErrors: Array<{ path: string; error: string }>;
	agentsFiles: Array<{ path: string }>;
	systemPromptSource?: { path: string };
	appendSystemPromptSources: Array<{ path: string }>;
}

// ============================================================================
// RPC State
// ============================================================================

export interface RpcSessionState {
	model?: Model<any>;
	thinkingLevel: ThinkingLevel;
	isStreaming: boolean;
	isCompacting: boolean;
	steeringMode: "all" | "one-at-a-time";
	followUpMode: "all" | "one-at-a-time";
	/** Models offered by the cycle-model UI, with their preferred thinking levels. */
	scopedModels: Array<{ model: Model<any>; thinkingLevel?: ThinkingLevel }>;
	sessionFile?: string;
	sessionId: string;
	sessionName?: string;
	autoCompactionEnabled: boolean;
	messageCount: number;
	pendingMessageCount: number;
}

// ============================================================================
// RPC Responses (stdout)
// ============================================================================

// Success responses with data
export type RpcResponse =
	// Prompting (async - events follow)
	| { id?: string; type: "response"; command: "prompt"; success: true }
	| { id?: string; type: "response"; command: "steer"; success: true }
	| { id?: string; type: "response"; command: "follow_up"; success: true }
	| { id?: string; type: "response"; command: "abort"; success: true }
	| { id?: string; type: "response"; command: "new_session"; success: true; data: { cancelled: boolean } }

	// State
	| { id?: string; type: "response"; command: "get_state"; success: true; data: RpcSessionState }

	// Model
	| {
			id?: string;
			type: "response";
			command: "set_model";
			success: true;
			data: Model<any>;
	  }
	| {
			id?: string;
			type: "response";
			command: "cycle_model";
			success: true;
			data: { model: Model<any>; thinkingLevel: ThinkingLevel; isScoped: boolean } | null;
	  }
	| {
			id?: string;
			type: "response";
			command: "get_available_models";
			success: true;
			data: { models: Model<any>[] };
	  }

	// Thinking
	| { id?: string; type: "response"; command: "set_thinking_level"; success: true }
	| {
			id?: string;
			type: "response";
			command: "cycle_thinking_level";
			success: true;
			data: { level: ThinkingLevel } | null;
	  }
	| {
			id?: string;
			type: "response";
			command: "get_available_thinking_levels";
			success: true;
			data: { levels: ThinkingLevel[] };
	  }

	// Queue modes
	| { id?: string; type: "response"; command: "set_steering_mode"; success: true }
	| { id?: string; type: "response"; command: "set_follow_up_mode"; success: true }

	// Compaction
	| { id?: string; type: "response"; command: "compact"; success: true; data: CompactionResult }
	| { id?: string; type: "response"; command: "set_auto_compaction"; success: true }

	// Retry
	| { id?: string; type: "response"; command: "set_auto_retry"; success: true }
	| { id?: string; type: "response"; command: "abort_retry"; success: true }

	// Bash
	| { id?: string; type: "response"; command: "bash"; success: true; data: BashResult }
	| { id?: string; type: "response"; command: "abort_bash"; success: true }

	// Session
	| { id?: string; type: "response"; command: "get_session_stats"; success: true; data: SessionStats }
	| { id?: string; type: "response"; command: "export_html"; success: true; data: { path: string } }
	| {
			id?: string;
			type: "response";
			command: "switch_session";
			success: true;
			data: { cancelled: boolean };
	  }
	| { id?: string; type: "response"; command: "delete_session"; success: true }
	| { id?: string; type: "response"; command: "fork"; success: true; data: { text: string; cancelled: boolean } }
	| { id?: string; type: "response"; command: "clone"; success: true; data: { cancelled: boolean } }
	| {
			id?: string;
			type: "response";
			command: "get_fork_messages";
			success: true;
			data: { messages: Array<{ entryId: string; text: string }> };
	  }
	| {
			id?: string;
			type: "response";
			command: "get_entries";
			success: true;
			data: { entries: SessionEntry[]; leafId: string | null };
	  }
	| {
			id?: string;
			type: "response";
			command: "get_tree";
			success: true;
			data: { tree: SessionTreeNode[]; leafId: string | null };
	  }
	| {
			id?: string;
			type: "response";
			command: "get_last_assistant_text";
			success: true;
			data: { text: string | null };
	  }
	| { id?: string; type: "response"; command: "set_session_name"; success: true }

	// Messages
	| {
			id?: string;
			type: "response";
			command: "get_messages";
			success: true;
			/**
			 * messages plus the message_end sequence HIGH-WATER MARK: every
			 * message_end event stamped seq <= messageSeq completed before
			 * this snapshot was taken and is included in it. message_end
			 * events carry the stamp as a `seq` field on the wire event.
			 */
			data: { messages: AgentMessage[]; messageSeq?: number };
	  }

	// Commands
	| {
			id?: string;
			type: "response";
			command: "get_commands";
			success: true;
			data: { commands: RpcSlashCommand[] };
	  }

	// Lifecycle
	| { id?: string; type: "response"; command: "shutdown"; success: true }
	| { id?: string; type: "response"; command: "detach"; success: true }

	// Remote-attach additions (P2)
	| {
			id?: string;
			type: "response";
			command: "get_context_usage";
			success: true;
			data: ContextUsage | null;
	  }
	| {
			id?: string;
			type: "response";
			command: "get_system_prompt";
			success: true;
			data: { systemPrompt: string };
	  }
	| { id?: string; type: "response"; command: "get_tools"; success: true; data: { tools: ToolInfo[] } }
	| { id?: string; type: "response"; command: "get_resources"; success: true; data: RpcResources }
	| { id?: string; type: "response"; command: "set_scoped_models"; success: true }
	| {
			id?: string;
			type: "response";
			command: "navigate_tree";
			success: true;
			data: { cancelled: boolean; editorText?: string };
	  }
	| { id?: string; type: "response"; command: "reload"; success: true }
	| { id?: string; type: "response"; command: "export_jsonl"; success: true; data: { path: string } }
	| { id?: string; type: "response"; command: "abort_compaction"; success: true }
	| { id?: string; type: "response"; command: "abort_branch_summary"; success: true }
	| {
			id?: string;
			type: "response";
			command: "clear_queue";
			success: true;
			data: { steering: string[]; followUp: string[] };
	  }
	| {
			id?: string;
			type: "response";
			command: "get_auth_status";
			success: true;
			data: RpcAuthStatus;
	  }
	| { id?: string; type: "response"; command: "refresh_models"; success: true }
	| { id?: string; type: "response"; command: "login"; success: true }
	| { id?: string; type: "response"; command: "logout"; success: true }
	| {
			id?: string;
			type: "response";
			command: "import_session";
			success: true;
			data: {
				cancelled: boolean;
				/** Set when the imported session's recorded cwd does not exist; the client should prompt and retry with cwdOverride. */
				missingCwd?: { sessionFile?: string; sessionCwd: string; fallbackCwd: string };
			};
	  }
	| { id?: string; type: "response"; command: "rename_session"; success: true }

	// Filesystem
	| {
			id?: string;
			type: "response";
			command: "fs_complete";
			success: true;
			data: { entries: Array<{ path: string; isDirectory: boolean }> };
	  }
	| {
			id?: string;
			type: "response";
			command: "read_file";
			success: true;
			data: { path: string; content: string; truncated: boolean };
	  }

	// Sessions
	| {
			id?: string;
			type: "response";
			command: "list_sessions";
			success: true;
			data: { sessions: RpcSessionInfo[] };
	  }

	// Error response (any command can fail)
	| {
			id?: string;
			type: "response";
			command: string;
			success: false;
			error: string;
			/**
			 * Structured MissingSessionCwdError payload (switch_session): lets the
			 * client reconstruct the typed error so the TUI can run its
			 * missing-cwd prompt/retry flow instead of treating it as fatal.
			 */
			missingCwd?: { sessionFile?: string; sessionCwd: string; fallbackCwd: string };
	  };

// ============================================================================
// RPC Server Events (stdout, not tied to a command)
// ============================================================================

/**
 * First line emitted when a client attaches. Clients should verify
 * `protocol` before sending commands.
 */
export interface RpcHello {
	type: "hello";
	protocol: number;
	/** pi version string */
	version: string;
	sessionId: string;
	cwd: string;
	capabilities: string[];
}

/** Emitted when the server ends a client attachment (e.g. another client took over). */
export interface RpcDetachedEvent {
	type: "detached";
	reason: "takeover" | "shutdown" | "detach";
}

/**
 * Emitted when the server rebinds to a different session (e.g. an
 * extension invoked newSession/switchSession/fork server-side). Clients
 * mirroring session state should refetch on this event.
 */
export interface RpcSessionChangedEvent {
	type: "session_changed";
	sessionId: string;
	cwd: string;
}

/**
 * Emitted while a login command is in flight when the provider's auth flow
 * needs user input. The client answers with an auth_response command whose
 * requestId matches the login command's id.
 */
export interface RpcAuthPromptEvent {
	type: "auth_prompt";
	requestId: string;
	prompt:
		| { kind: "text"; message: string; placeholder?: string }
		| { kind: "secret"; message: string; placeholder?: string }
		| { kind: "manual_code"; message: string; placeholder?: string }
		| { kind: "select"; message: string; options: Array<{ id: string; label: string; description?: string }> };
}

/** Fire-and-forget progress/info during a login flow (auth URLs, device codes, progress). */
export interface RpcAuthNotifyEvent {
	type: "auth_notify";
	requestId: string;
	/** AuthEvent from pi-ai (plain JSON: info/auth_url/device_code/progress). */
	event: unknown;
}

/** Emitted when an extension event handler throws on the agent host. */
export interface RpcExtensionErrorEvent {
	type: "extension_error";
	extensionPath: string;
	event: string;
	error: string;
}

/** Serializable form of SessionInfo (dates as ISO strings). */
export interface RpcSessionInfo {
	path: string;
	id: string;
	cwd: string;
	name?: string;
	parentSessionPath?: string;
	created: string;
	modified: string;
	messageCount: number;
	firstMessage: string;
}

// ============================================================================
// Extension UI Events (stdout)
// ============================================================================

/** Emitted when an extension needs user input */
export type RpcExtensionUIRequest =
	| { type: "extension_ui_request"; id: string; method: "select"; title: string; options: string[]; timeout?: number }
	| { type: "extension_ui_request"; id: string; method: "confirm"; title: string; message: string; timeout?: number }
	| {
			type: "extension_ui_request";
			id: string;
			method: "input";
			title: string;
			placeholder?: string;
			timeout?: number;
	  }
	| { type: "extension_ui_request"; id: string; method: "editor"; title: string; prefill?: string }
	| {
			type: "extension_ui_request";
			id: string;
			method: "notify";
			message: string;
			notifyType?: "info" | "warning" | "error";
	  }
	| {
			type: "extension_ui_request";
			id: string;
			method: "setStatus";
			statusKey: string;
			statusText: string | undefined;
	  }
	| {
			type: "extension_ui_request";
			id: string;
			method: "setWidget";
			widgetKey: string;
			widgetLines: string[] | undefined;
			widgetPlacement?: "aboveEditor" | "belowEditor";
	  }
	| { type: "extension_ui_request"; id: string; method: "setTitle"; title: string }
	| { type: "extension_ui_request"; id: string; method: "set_editor_text"; text: string };

// ============================================================================
// Extension UI Commands (stdin)
// ============================================================================

/** Response to an extension UI request */
export type RpcExtensionUIResponse =
	| { type: "extension_ui_response"; id: string; value: string }
	| { type: "extension_ui_response"; id: string; confirmed: boolean }
	| { type: "extension_ui_response"; id: string; cancelled: true };

// ============================================================================
// Helper type for extracting command types
// ============================================================================

export type RpcCommandType = RpcCommand["type"];
