/**
 * RPC mode: Headless operation with JSON stdin/stdout protocol.
 *
 * Used for embedding the agent in other applications.
 * Receives commands as JSON on stdin, outputs events and responses as JSON on stdout.
 *
 * Protocol:
 * - Commands: JSON objects with `type` field, optional `id` for correlation
 * - Responses: JSON objects with `type: "response"`, `command`, `success`, and optional `data`/`error`
 * - Events: AgentSessionEvent objects streamed as they occur
 * - Extension UI: Extension UI requests are emitted, client responds with extension_ui_response
 * - Lifecycle: a `hello` greeting is emitted first; `shutdown` terminates the
 *   agent, `detach` ends the attachment
 *
 * This module is the stdio adapter; the protocol logic lives in rpc-server.ts.
 */

import type { AgentSessionRuntime } from "../../core/agent-session-runtime.ts";
import {
	flushRawStdout,
	takeOverStdout,
	waitForRawStdoutBackpressure,
	writeRawStdout,
} from "../../core/output-guard.ts";
import { killTrackedDetachedChildren } from "../../utils/shell.ts";
import { attachJsonlLineReader, serializeJsonLine } from "./jsonl.ts";
import { type RpcConnection, RpcServer } from "./rpc-server.ts";

// Re-export types for consumers
export type {
	RpcCommand,
	RpcExtensionUIRequest,
	RpcExtensionUIResponse,
	RpcResponse,
	RpcSessionState,
} from "./rpc-types.ts";

/**
 * Run in RPC mode over stdio.
 * Listens for JSON commands on stdin, outputs events and responses on stdout.
 * Process exits when stdin closes or a `shutdown` command arrives.
 */
export async function runRpcMode(runtimeHost: AgentSessionRuntime): Promise<never> {
	takeOverStdout();

	const signalCleanupHandlers: Array<() => void> = [];
	let shuttingDown = false;

	async function shutdown(exitCode = 0, signal?: NodeJS.Signals): Promise<never> {
		if (shuttingDown) {
			process.exit(exitCode);
		}
		shuttingDown = true;
		for (const cleanup of signalCleanupHandlers) {
			cleanup();
		}
		detachInput();
		process.stdin.pause();
		if (signal !== "SIGTERM") {
			await flushRawStdout();
		}
		process.exit(exitCode);
	}

	const server = new RpcServer(runtimeHost, {
		connectionLoss: "shutdown",
		onShutdown: (exitCode) => shutdown(exitCode),
	});

	const connection: RpcConnection = {
		send: (obj) => {
			writeRawStdout(serializeJsonLine(obj));
		},
		onLine: (cb) => {
			detachInput = attachJsonlLineReader(process.stdin, cb);
		},
		onClose: (cb) => {
			process.stdin.on("end", cb);
		},
		close: () => {
			process.stdin.pause();
		},
		flush: flushRawStdout,
	};

	let detachInput: () => void = () => {};

	const registerSignalHandlers = (): void => {
		const signals: NodeJS.Signals[] = ["SIGTERM"];
		if (process.platform !== "win32") {
			signals.push("SIGHUP");
		}

		for (const signal of signals) {
			const handler = () => {
				killTrackedDetachedChildren();
				const exitCode = signal === "SIGHUP" ? 129 : 143;
				// Route through RpcServer.shutdown so runtimeHost.dispose() runs
				// (session_shutdown hooks, extension cleanup); onShutdown reaches
				// the local shutdown() for stdout flush and exit.
				void server.shutdown(exitCode);
				// Never let a hung dispose trap the process on a signal.
				setTimeout(() => process.exit(exitCode), 5000).unref();
			};
			process.on(signal, handler);
			signalCleanupHandlers.push(() => process.off(signal, handler));
		}
	};

	await server.start();

	// Backpressure: pause event output when stdout is congested.
	let unsubscribeBackpressure = server.subscribeAgentEvents(async () => {
		await waitForRawStdoutBackpressure();
	});
	signalCleanupHandlers.push(() => {
		unsubscribeBackpressure();
		unsubscribeBackpressure = () => {};
	});

	registerSignalHandlers();
	server.attachConnection(connection);

	// Keep process alive forever
	return new Promise(() => {});
}
