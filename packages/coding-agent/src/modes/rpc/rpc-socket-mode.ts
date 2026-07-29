/**
 * RPC socket mode: like RPC mode, but serving on a unix socket.
 *
 * The agent outlives its clients: clients attach, detach, and reattach while
 * the session keeps running. A second client takes over from the first.
 * When a connection is lost without `detach`, extension UI requests are held
 * for a grace window (default 30s) and re-emitted to a reconnecting client.
 *
 * Access control relies on socket file permissions (0600): only the owning
 * user can connect. Peer-credential verification is follow-up work.
 */

import { chmodSync, unlinkSync } from "node:fs";
import { createServer, type Server, Socket } from "node:net";
import type { AgentSessionRuntime } from "../../core/agent-session-runtime.ts";
import { killTrackedDetachedChildren } from "../../utils/shell.ts";
import { attachJsonlLineReader, serializeJsonLine } from "./jsonl.ts";
import { type RpcConnection, RpcServer } from "./rpc-server.ts";

export interface RpcSocketModeOptions {
	/** Path of the unix socket to listen on (named pipe on Windows). */
	socketPath: string;
	/** Grace window for connection loss before UI requests auto-resolve. */
	detachGraceMs?: number;
	/**
	 * Called after server-initiated shutdown (shutdown command, extension
	 * shutdown request) once the listener is closed. runRpcSocketMode uses
	 * this to exit the process; tests omit it.
	 */
	onShutdown?: (exitCode: number) => void | Promise<void>;
}

export interface RpcSocketServer {
	server: RpcServer;
	listener: Server;
	/** Close the listener and unlink the socket file. Does not exit the process. */
	close(): Promise<void>;
}

function createSocketConnection(socket: Socket): RpcConnection {
	// Serialize writes; respect stream backpressure.
	let writeChain: Promise<void> = Promise.resolve();
	const connection: RpcConnection = {
		send: (obj) => {
			const line = serializeJsonLine(obj);
			writeChain = writeChain.then(
				() =>
					new Promise<void>((resolve) => {
						if (socket.destroyed || socket.writableEnded) {
							resolve();
							return;
						}
						try {
							socket.write(line, () => resolve());
						} catch {
							// Peer vanished mid-write; nothing more to do.
							resolve();
						}
					}),
			);
		},
		onLine: (cb) => {
			attachJsonlLineReader(socket, cb);
		},
		onClose: (cb) => {
			socket.once("close", cb);
		},
		close: () => {
			// Flush queued writes before ending so a final message (e.g. the
			// takeover notice) is not lost.
			void writeChain.then(() => {
				if (!socket.destroyed && !socket.writableEnded) socket.end();
			});
			// If the peer never finishes, force-close so onClose still fires.
			setTimeout(() => {
				if (!socket.destroyed) socket.destroy();
			}, 1000).unref();
		},
		flush: () => writeChain,
	};
	return connection;
}

/**
 * Refuse to steal a live socket; clean up a stale one.
 */
async function claimSocketPath(socketPath: string): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		const probe = new Socket();
		probe.once("error", (err: NodeJS.ErrnoException) => {
			if (err.code === "ECONNREFUSED" || err.code === "ENOENT") {
				// Stale socket (or nothing there): safe to remove.
				try {
					unlinkSync(socketPath);
				} catch {
					// Nothing to remove.
				}
				resolve();
			} else {
				reject(err);
			}
		});
		probe.once("connect", () => {
			probe.destroy();
			reject(new Error(`Socket ${socketPath} is already in use by a live process`));
		});
		probe.connect(socketPath);
	});
}

function cleanupSocketFile(socketPath: string): void {
	if (process.platform === "win32") return;
	try {
		unlinkSync(socketPath);
	} catch {
		// Already gone.
	}
}

/**
 * Create a listening RPC socket server. Does not install signal handlers or
 * exit the process unless `options.onShutdown` does so.
 */
export async function createRpcSocketServer(
	runtimeHost: AgentSessionRuntime,
	options: RpcSocketModeOptions,
): Promise<RpcSocketServer> {
	const { socketPath } = options;
	let listener: Server | undefined;

	const close = async (): Promise<void> => {
		const current = listener;
		if (!current) return;
		listener = undefined;
		await new Promise<void>((resolve) => {
			current.close(() => resolve());
			// close() only fires once all connections end; don't wait forever.
			setTimeout(resolve, 2000).unref();
		});
		cleanupSocketFile(socketPath);
	};

	const rpcServer = new RpcServer(runtimeHost, {
		connectionLoss: "grace",
		detachGraceMs: options.detachGraceMs,
		onShutdown: async (exitCode) => {
			await close();
			await options.onShutdown?.(exitCode);
		},
	});

	await rpcServer.start();

	await claimSocketPath(socketPath);

	listener = createServer((socket) => {
		// Never let a misbehaving or abruptly-reset peer crash the server:
		// 'error' (e.g. ECONNRESET) is always followed by 'close', which
		// drives the connection-loss path in RpcServer.
		socket.on("error", () => {});
		rpcServer.attachConnection(createSocketConnection(socket));
	});

	await new Promise<void>((resolve, reject) => {
		listener!.once("error", reject);
		listener!.listen(socketPath, () => resolve());
	});

	if (process.platform !== "win32") {
		try {
			chmodSync(socketPath, 0o600);
		} catch {
			// Best effort; filesystem may not support permissions.
		}
	}

	return { server: rpcServer, listener, close };
}

export async function runRpcSocketMode(
	runtimeHost: AgentSessionRuntime,
	options: RpcSocketModeOptions,
): Promise<never> {
	let shuttingDown = false;

	const shutdown = async (exitCode: number): Promise<never> => {
		if (!shuttingDown) {
			shuttingDown = true;
			// Listener close happens via the socket server's onShutdown chain
			// (or here for signal-driven shutdown).
			await socketServer?.close();
		}
		process.exit(exitCode);
	};

	const socketServer = await createRpcSocketServer(runtimeHost, {
		...options,
		onShutdown: (exitCode) => shutdown(exitCode),
	});

	const signals: NodeJS.Signals[] = ["SIGTERM"];
	if (process.platform !== "win32") {
		signals.push("SIGHUP");
	}
	for (const signal of signals) {
		process.on(signal, () => {
			killTrackedDetachedChildren();
			void shutdown(signal === "SIGHUP" ? 129 : 143);
		});
	}

	// Keep process alive forever
	return new Promise(() => {});
}
