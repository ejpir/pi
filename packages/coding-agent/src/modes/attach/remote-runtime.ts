/**
 * RemoteAgentSessionRuntime — a facade over RpcClient implementing the
 * AgentSessionRuntime surface the stock interactive TUI binds to.
 *
 * Owns the RemoteAgentSession plus the rebind flow: when the server rebinds
 * to a different session (extension-invoked newSession/switchSession/fork,
 * or client-issued ones), it emits `session_changed`; this runtime refetches
 * the mirror and invokes the TUI's rebind callback, mirroring the in-process
 * contract of AgentSessionRuntime.
 */

import { existsSync, readFileSync } from "node:fs";
import { basename } from "node:path";
import type { AgentSession } from "../../core/agent-session.ts";
import {
	type AgentSessionRuntime,
	SessionImportError,
	SessionImportFileNotFoundError,
	SessionImportUnsupportedError,
} from "../../core/agent-session-runtime.ts";
import type { AgentSessionServices } from "../../core/agent-session-services.ts";
import type { ModelRuntime } from "../../core/model-runtime.ts";
import type { ResourceLoader } from "../../core/resource-loader.ts";
import { MissingSessionCwdError } from "../../core/session-cwd.ts";
import type { SettingsManager } from "../../core/settings-manager.ts";
import { resolvePath } from "../../utils/paths.ts";
import type { RpcClient } from "../rpc/rpc-client.ts";
import type { RemoteAgentSession } from "./remote-agent-session.ts";

export interface RemoteRuntimeOptions {
	client: RpcClient;
	session: RemoteAgentSession;
	/** Client-local settings manager (UI-scoped settings). */
	settingsManager: SettingsManager;
	/** Client-local agent dir (used for project trust stores etc.). */
	agentDir: string;
}

/** How long session-replacing commands wait for the rebind event before giving up. */
const REBIND_TIMEOUT_MS = 10_000;

export class RemoteAgentSessionRuntime {
	private readonly client: RpcClient;
	private readonly remoteSession: RemoteAgentSession;
	private readonly options: RemoteRuntimeOptions;

	private rebindCb: ((session: AgentSession) => Promise<void>) | undefined;
	private beforeInvalidateCb: (() => void) | undefined;
	private rebinding = false;
	/** Set when a session_changed arrives mid-rebind; coalesced and re-run. */
	private rebindPending = false;
	private rebindWaiter: (() => void) | undefined;

	constructor(options: RemoteRuntimeOptions) {
		this.options = options;
		this.client = options.client;
		this.remoteSession = options.session;
		this.remoteSession.onSessionChanged = () => void this.handleSessionChanged();
	}

	get session(): AgentSession {
		return this.remoteSession as unknown as AgentSession;
	}

	get services(): AgentSessionServices {
		return {
			cwd: this.cwd,
			agentDir: this.options.agentDir,
			modelRuntime: this.remoteSession.modelRuntime as unknown as ModelRuntime,
			settingsManager: this.options.settingsManager,
			resourceLoader: this.remoteSession.resourceLoader as unknown as ResourceLoader,
			diagnostics: [],
		};
	}

	get cwd(): string {
		return this.remoteSession.sessionManager.getCwd();
	}

	get diagnostics(): readonly unknown[] {
		return [];
	}

	get modelFallbackMessage(): string | undefined {
		return undefined;
	}

	setRebindSession(callback: (session: AgentSession) => Promise<void>): void {
		this.rebindCb = callback;
	}

	setBeforeSessionInvalidate(callback: () => void): void {
		this.beforeInvalidateCb = callback;
	}

	async newSession(options?: { parentSession?: string }): Promise<{ cancelled: boolean }> {
		const rebind = this.expectRebind();
		const result = await this.client.newSession(options?.parentSession);
		if (!result.cancelled) await rebind;
		return { cancelled: result.cancelled };
	}

	async switchSession(sessionPath: string): Promise<{ cancelled: boolean }> {
		const rebind = this.expectRebind();
		const result = await this.client.switchSession(sessionPath);
		if (!result.cancelled) await rebind;
		return { cancelled: result.cancelled };
	}

	async fork(
		entryId: string,
		_options?: { createCopy?: boolean },
	): Promise<{ cancelled: boolean; selectedText?: string }> {
		const rebind = this.expectRebind();
		const result = await this.client.fork(entryId);
		if (!result.cancelled) await rebind;
		return { cancelled: result.cancelled, selectedText: result.text };
	}

	/**
	 * /import over the wire. Path resolution precedence:
	 *   1. resolve against the CLIENT's cwd (the user typed the path in
	 *      their own shell) and read locally;
	 *   2. on a local miss, read agent-side over read_file — absolute and
	 *      "~/" paths name the agent host, a relative path resolves against
	 *      the agent's session cwd.
	 * If the path exists on both sides the client copy silently wins.
	 * The JSONL content is then uploaded into the agent's session directory
	 * and the agent switches to it — the resulting session_changed rebind
	 * lands the TUI on the imported session.
	 */
	async importFromJsonl(inputPath: string, cwdOverride?: string): Promise<{ cancelled: boolean }> {
		// Pre-flight: an agent running an older build (e.g. a stale baked
		// container image) lacks import_session — say so plainly instead of
		// surfacing the wire's "Unknown command" error.
		if (!this.client.hasCapability("import_session")) {
			throw new SessionImportUnsupportedError(
				"The attached agent does not support /import (missing import_session capability) — update or rebuild the agent image",
			);
		}
		const clientPath = resolvePath(inputPath, process.cwd());
		let content: string;
		let fileName: string;
		if (existsSync(clientPath)) {
			// Every throw from here down must stay inside the SessionImportError
			// hierarchy — anything else routes the stock handler to its fatal
			// path and kills the attach for a recoverable condition.
			try {
				content = readFileSync(clientPath, "utf8");
			} catch (error) {
				throw new SessionImportError(error instanceof Error ? error.message : String(error));
			}
			fileName = basename(clientPath);
		} else {
			let agentFile: { path: string; content: string; truncated: boolean };
			try {
				agentFile = await this.client.readFile(inputPath);
			} catch (err) {
				// Only a genuine miss becomes "not found" at the client path.
				// read_file also fails with "Not a file"/"Binary file", and the
				// RPC itself can time out or drop — those are agent-side errors
				// and must surface as themselves, not as a misleading local
				// "file not found".
				const message = err instanceof Error ? err.message : String(err);
				if (message.includes("File not found")) {
					throw new SessionImportFileNotFoundError(clientPath);
				}
				throw new SessionImportError(`Failed to read ${inputPath} on the agent host: ${message}`);
			}
			if (agentFile.truncated) {
				throw new SessionImportError(`Session file too large to import over the wire: ${agentFile.path}`);
			}
			content = agentFile.content;
			fileName = basename(agentFile.path);
		}

		// Fail fast with a clear message for the common trap: /import reads
		// JSONL session files; an HTML export is not importable. (Hoisted
		// into the agent-side import too, so local /import gets the same
		// guard.)
		const trimmed = content.trimStart();
		if (trimmed.length === 0) {
			throw new SessionImportError("Session file is empty");
		}
		if (!trimmed.startsWith("{")) {
			throw new SessionImportError(
				"Not a session JSONL file — export with `/export <file>.jsonl` and import that (HTML exports are not importable)",
			);
		}

		let result: {
			cancelled: boolean;
			missingCwd?: { sessionFile?: string; sessionCwd: string; fallbackCwd: string };
		};
		try {
			result = await this.client.importSession({ content, fileName, cwdOverride });
		} catch (error) {
			// The stock import handler routes unclassified errors to
			// handleFatalRuntimeError (process.exit) — survivable locally,
			// but over the wire a transport timeout or an agent-side failure
			// must not kill the attach. Report as unsupported: the TUI shows
			// the message and stays alive.
			throw new SessionImportUnsupportedError(error instanceof Error ? error.message : String(error));
		}
		if (result.missingCwd) {
			// Reconstruct the typed error so the TUI's stock retry flow
			// (prompt for a cwd, retry with cwdOverride) works unchanged.
			throw new MissingSessionCwdError(result.missingCwd);
		}
		return { cancelled: result.cancelled };
	}

	async dispose(): Promise<void> {
		this.remoteSession.dispose();
		await this.client.stop();
	}

	// =========================================================================
	// Rebind flow
	// =========================================================================

	private expectRebind(): Promise<void> {
		return new Promise<void>((resolve) => {
			const timer = setTimeout(() => {
				if (this.rebindWaiter === waiter) this.rebindWaiter = undefined;
				resolve();
			}, REBIND_TIMEOUT_MS);
			const waiter = () => {
				clearTimeout(timer);
				resolve();
			};
			// A second session-replacing call while one awaits its rebind:
			// resolve the first waiter rather than stranding it for the full
			// timeout — the upcoming rebind covers both.
			const previous = this.rebindWaiter;
			this.rebindWaiter = waiter;
			previous?.();
		});
	}

	/**
	 * Rebind the TUI after the client reconnected to a (restarted) agent:
	 * invalidate session-derived UI state, refetch the mirror from the new
	 * server, then rebind. The session on the other end may be a brand-new
	 * one or the same session file resumed — the mirror reflects either.
	 */
	async handleReconnect(): Promise<void> {
		await this.handleSessionChanged();
	}

	private async rebindFromMirror(): Promise<void> {
		try {
			this.beforeInvalidateCb?.();
			await this.remoteSession.refetchAll();
			await this.rebindCb?.(this.session);
		} catch (rebindError: unknown) {
			// Rebind failures must not kill the event loop; the mirror is
			// already refetched, so the TUI stays usable. Still log — a silent
			// failure here otherwise presents as a mysteriously stale TUI.
			console.error(
				"attach: session rebind failed:",
				rebindError instanceof Error ? rebindError.message : rebindError,
			);
		}
	}

	private async handleSessionChanged(): Promise<void> {
		if (this.rebinding) {
			// A second session change raced the in-flight rebind: coalesce it
			// and re-run once, so the TUI never stays bound to an intermediate
			// session. The waiter is still signalled below.
			this.rebindPending = true;
			return;
		}
		this.rebinding = true;
		try {
			do {
				this.rebindPending = false;
				await this.rebindFromMirror();
			} while (this.rebindPending);
		} finally {
			this.rebinding = false;
			const waiter = this.rebindWaiter;
			this.rebindWaiter = undefined;
			waiter?.();
		}
	}
}

/** Structural assertion helper: the runtime satisfies the TUI's host contract. */
export function asAgentSessionRuntime(runtime: RemoteAgentSessionRuntime): AgentSessionRuntime {
	return runtime as unknown as AgentSessionRuntime;
}
