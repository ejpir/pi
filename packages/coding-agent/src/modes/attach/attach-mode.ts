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
	if (options.command && options.socketPath) {
		throw new Error("--cmd and --sock are mutually exclusive");
	}

	const client = new RpcClient(
		options.socketPath ? { socketPath: options.socketPath } : { command: options.command! },
	);
	await client.start();

	// Everything between start() and the interactive run must not leak the
	// spawned agent process / socket connection on failure.
	const stopClientQuietly = async (): Promise<void> => {
		await client.stop().catch(() => {});
	};

	let remote: RemoteAgentSession;
	let runtime: RemoteAgentSessionRuntime;
	let settingsManager: SettingsManager;
	try {
		const hello = client.getHello();
		if (!hello) {
			throw new Error("agent did not complete the hello handshake");
		}

		// Client-local settings: theme, keybindings, editor prefs stay on the
		// attach host (see RFC "settings split").
		settingsManager = SettingsManager.create(options.cwd, options.agentDir);
		initTheme(settingsManager.getTheme(), true);

		remote = await RemoteAgentSession.connect({ client, settingsManager });
		runtime = new RemoteAgentSessionRuntime({
			client,
			session: remote,
			settingsManager,
			agentDir: options.agentDir,
		});
	} catch (setupError: unknown) {
		await stopClientQuietly();
		throw setupError;
	}

	let detachReason: string | undefined;
	let tuiStopped = false;
	let exitScheduled = false;
	let interactiveRef: InteractiveMode | undefined;

	/**
	 * Forced teardown for server-pushed detach (takeover, connection lost).
	 * InteractiveMode.stop() restores the terminal but does NOT settle run()
	 * (its pending input never resolves), so the only reliable way out is
	 * process exit — consistent with every other TUI exit path.
	 */
	const forceExit = (reason: string, exitCode: number): void => {
		if (exitScheduled) return;
		exitScheduled = true;
		detachReason = reason;
		tuiStopped = true;
		try {
			interactiveRef?.stop();
		} catch {
			// Terminal may already be restored.
		}
		console.error(`\nDetached from agent: ${reason}`);
		void stopClientQuietly().finally(() => process.exit(exitCode));
	};

	remote.onDetached = (reason) => {
		// Server-initiated: takeover means another client owns the agent now —
		// exit immediately. A graceful shutdown is followed by a CLEAN transport
		// close (error === null), which the reconnect loop below would treat as
		// "not a failure" and ignore — so exit here too. tuiStopped guards our
		// own exit flow (we sent the shutdown ourselves).
		if (reason === "takeover") {
			forceExit(reason, 2);
		} else if (reason === "shutdown" && !tuiStopped) {
			forceExit(reason, 0);
		}
	};

	// Reconnect loop: when the socket drops (agent restarted, crashed, or
	// shut down), keep redialing. A restarted agent gets a fresh mirror and
	// the TUI rebinds to whatever session the new server holds.
	let reconnecting = false;
	client.onClose(() => {
		if (tuiStopped || reconnecting) return;
		// A close without a preceding detached event — clean closes included,
		// e.g. a --cmd bridge exiting 0 after the agent behind it crashed —
		// means the transport died underneath us. Treat it as connection loss
		// and redial like any other drop.
		reconnecting = true;
		void (async () => {
			const deadline = Date.now() + 30_000;
			while (!tuiStopped && Date.now() < deadline) {
				try {
					await client.reconnect();
					await runtime.handleReconnect();
					reconnecting = false;
					return;
				} catch {
					await new Promise((resolve) => setTimeout(resolve, 500));
				}
			}
			if (!tuiStopped) {
				forceExit("connection lost", 1);
			}
		})();
	});

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
		deleteSession: async (sessionPath) => {
			// Delete on the AGENT host: the picker's paths name agent-side files.
			// Local trash/unlink would falsely report success for absent paths or
			// delete an unrelated client file on a path collision.
			try {
				await client.deleteSession(sessionPath);
				return { ok: true };
			} catch (error: unknown) {
				return { ok: false, error: error instanceof Error ? error.message : String(error) };
			}
		},
	};

	const interactive = new InteractiveMode(asAgentSessionRuntime(runtime), {
		sessionPicker,
		// @-completion queries the AGENT's filesystem over fs_complete —
		// the agent's cwd (e.g. /work in a container) usually doesn't exist
		// on the attach host, so the default fd walker finds nothing there.
		fileCompletion: async (query, { signal }) => {
			try {
				const entries = await client.fsComplete(query);
				return signal.aborted ? [] : entries;
			} catch {
				return [];
			}
		},
		verbose: options.verbose,
	});
	interactiveRef = interactive;
	if (exitScheduled) {
		// Takeover raced ahead of TUI startup; forceExit already fired but had
		// no TUI to restore — stop() now for a clean terminal before exiting.
		interactive.stop();
	}

	try {
		await interactive.run();
	} finally {
		tuiStopped = true;
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
