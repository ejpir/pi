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
			} else if (err.code === "EACCES") {
				reject(
					new Error(
						`Cannot probe socket ${socketPath}: permission denied. ` +
							"It may belong to another user; remove it manually or choose a different --sock path.",
					),
				);
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

	const connections = new Set<Socket>();

	// Re-entrant: a concurrent second call awaits the same in-flight cleanup.
	let closePromise: Promise<void> | undefined;
	const close = (): Promise<void> => {
		if (closePromise) return closePromise;
		const current = listener;
		if (!current) return Promise.resolve();
		listener = undefined;
		closePromise = (async () => {
			await new Promise<void>((resolve) => {
				current.close(() => resolve());
				// close() only fires once all connections end; don't wait forever.
				setTimeout(resolve, 2000).unref();
				// Destroy accepted connections: mirrors process-exit semantics and
				// lets peers observe the close (in-process restarts, tests).
				for (const connection of connections) {
					connection.destroy();
				}
			});
			cleanupSocketFile(socketPath);
		})();
		return closePromise;
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
		connections.add(socket);
		socket.on("close", () => connections.delete(socket));
		socket.on("error", () => {});
		rpcServer.attachConnection(createSocketConnection(socket));
	});

	// The socket grants full agent control with no authentication, so it must
	// never be world-accessible: apply a restrictive umask around listen() so
	// the socket node is created 0600 (no race window), then chmod as a
	// belt-and-braces fix for filesystems that ignore umask.
	const previousUmask = process.platform !== "win32" ? process.umask(0o077) : 0;
	try {
		await new Promise<void>((resolve, reject) => {
			listener!.once("error", reject);
			listener!.listen(socketPath, () => resolve());
		});
	} finally {
		if (process.platform !== "win32") {
			process.umask(previousUmask);
		}
	}

	if (process.platform !== "win32") {
		try {
			chmodSync(socketPath, 0o600);
		} catch (chmodError: unknown) {
			console.error(
				`Warning: could not restrict permissions on ${socketPath}; the agent socket may be accessible to other users:`,
				chmodError,
			);
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
