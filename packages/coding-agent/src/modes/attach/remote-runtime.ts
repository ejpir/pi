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

import type { AgentSession } from "../../core/agent-session.ts";
import type { AgentSessionRuntime } from "../../core/agent-session-runtime.ts";
import type { AgentSessionServices } from "../../core/agent-session-services.ts";
import type { ModelRuntime } from "../../core/model-runtime.ts";
import type { ResourceLoader } from "../../core/resource-loader.ts";
import type { SettingsManager } from "../../core/settings-manager.ts";
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

	async importFromJsonl(): Promise<never> {
		throw new Error("Session import is not supported in attach mode — import on the agent host");
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
			this.rebindWaiter = waiter;
		});
	}

	private async handleSessionChanged(): Promise<void> {
		if (this.rebinding) return;
		this.rebinding = true;
		try {
			this.beforeInvalidateCb?.();
			await this.remoteSession.refetchAll();
			await this.rebindCb?.(this.session);
		} catch {
			// Rebind failures must not kill the event loop; the mirror is
			// already refetched, so the TUI stays usable.
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
