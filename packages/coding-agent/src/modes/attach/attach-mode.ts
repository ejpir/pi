/**
 * Attach mode: run the stock interactive TUI against a remote agent.
 *
 * The agent (session, LLM credentials, extensions, tools) lives behind an
 * RPC endpoint — either a long-lived socket server (`pi attach --sock`)
 * or a command spawned per attach (`pi attach --cmd`, e.g. a container or
 * VM exec bridge). This host only renders the UI: local SettingsManager
 * and agentDir provide theme/keybindings/trust stores, while every
 * session operation flows through RpcClient + the remote facades.
 *
 * Lifecycle: exiting the TUI sends `shutdown` for --cmd transports (the
 * agent is ours) and `detach` for --sock (the agent outlives clients).
 */

import type { SessionInfo } from "../../core/session-manager.ts";
import { SettingsManager } from "../../core/settings-manager.ts";
import { InteractiveMode, type SessionPickerHooks } from "../interactive/interactive-mode.ts";
import { initTheme, stopThemeWatcher } from "../interactive/theme/theme.ts";
import { RpcClient } from "../rpc/rpc-client.ts";
import type { RpcSessionInfo } from "../rpc/rpc-types.ts";
import { RemoteAgentSession } from "./remote-agent-session.ts";
import { asAgentSessionRuntime, RemoteAgentSessionRuntime } from "./remote-runtime.ts";

export interface AttachModeOptions {
	/** Shell command to spawn the agent with (exclusive with socketPath). */
	command?: string;
	/** Unix socket path of a running agent (exclusive with command). */
	socketPath?: string;
	/** Local cwd for the client-local settings stack. */
	cwd: string;
	/** Local agent dir (theme/keybindings/trust stores stay client-side). */
	agentDir: string;
	verbose?: boolean;
}

function toSessionInfo(entry: RpcSessionInfo): SessionInfo {
	return {
		path: entry.path,
		id: entry.id,
		cwd: entry.cwd,
		name: entry.name,
		parentSessionPath: entry.parentSessionPath,
		created: new Date(entry.created),
		modified: new Date(entry.modified),
		messageCount: entry.messageCount,
		firstMessage: entry.firstMessage,
		// Full-message search text is not carried over the wire; the picker
		// searches names/first messages only in attach mode.
		allMessagesText: entry.firstMessage,
	};
}

export async function runAttachMode(options: AttachModeOptions): Promise<void> {
	if (!options.command && !options.socketPath) {
		throw new Error("attach requires --cmd <command> or --sock <path>");
	}

	const client = new RpcClient(
		options.socketPath ? { socketPath: options.socketPath } : { command: options.command! },
	);
	await client.start();

	const hello = client.getHello();
	if (!hello) {
		await client.stop();
		throw new Error("agent did not complete the hello handshake");
	}

	// Client-local settings: theme, keybindings, editor prefs stay on the
	// attach host (see RFC "settings split").
	const settingsManager = SettingsManager.create(options.cwd, options.agentDir);
	initTheme(settingsManager.getTheme(), true);

	const remote = await RemoteAgentSession.connect({ client, settingsManager });
	const runtime = new RemoteAgentSessionRuntime({
		client,
		session: remote,
		settingsManager,
		agentDir: options.agentDir,
	});

	let detachReason: string | undefined;
	remote.onDetached = (reason) => {
		detachReason = reason;
	};

	const sessionPicker: SessionPickerHooks = {
		list: async (onProgress) => {
			const sessions = (await client.listSessions(false)).map(toSessionInfo);
			onProgress?.(sessions.length, sessions.length);
			return sessions;
		},
		listAll: async (onProgress) => {
			const sessions = (await client.listSessions(true)).map(toSessionInfo);
			onProgress?.(sessions.length, sessions.length);
			return sessions;
		},
		renameSession: async (sessionPath, nextName) => {
			const next = (nextName ?? "").trim();
			if (!next) return;
			await client.renameSession(sessionPath, next);
		},
	};

	const interactive = new InteractiveMode(asAgentSessionRuntime(runtime), {
		sessionPicker,
		verbose: options.verbose,
	});

	try {
		await interactive.run();
	} finally {
		stopThemeWatcher();
		if (detachReason) {
			// The server pushed us off (takeover or shutdown); transport is gone.
			console.error(`\nDetached from agent: ${detachReason}`);
		} else if (options.command) {
			// Per-attach agent: it is ours, shut it down with the session.
			await client.shutdown().catch(() => {});
		} else {
			// Long-lived agent: leave the session running for reattach.
			await client.detach().catch(() => {});
		}
		await client.stop().catch(() => {});
	}
}
