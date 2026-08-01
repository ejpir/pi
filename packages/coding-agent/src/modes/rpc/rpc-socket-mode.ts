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

import { chmodSync, lstatSync, unlinkSync } from "node:fs";
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
	/** Bound for the agent-side model-catalog refresh (see RpcServerOptions). */
	refreshTimeoutMs?: number;
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
			// Flush queued writes so a final message (e.g. the takeover notice) is not lost.
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
				// ECONNREFUSED also comes from regular files; only ever unlink a socket.
				try {
					const stat = lstatSync(socketPath);
					if (!stat.isSocket()) {
						reject(
							new Error(
								`Refusing to remove non-socket file at ${socketPath}. ` +
									"Remove it yourself or choose a different --sock path.",
							),
						);
						return;
					}
					unlinkSync(socketPath);
				} catch (statError: unknown) {
					if ((statError as NodeJS.ErrnoException).code !== "ENOENT") {
						reject(statError);
						return;
					}
					// Nothing there.
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
		// A same-path replacement that is not a socket is not ours to delete.
		if (!lstatSync(socketPath).isSocket()) return;
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
				// Destroy accepted connections so peers observe the close.
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
		refreshTimeoutMs: options.refreshTimeoutMs,
		onShutdown: async (exitCode) => {
			await close();
			await options.onShutdown?.(exitCode);
		},
	});

	await rpcServer.start();

	await claimSocketPath(socketPath);

	listener = createServer((socket) => {
		// A peer reset must never crash the server: 'error' (e.g. ECONNRESET)
		// is always followed by 'close', which drives the connection-loss path.
		connections.add(socket);
		socket.on("close", () => connections.delete(socket));
		socket.on("error", () => {});
		rpcServer.attachConnection(createSocketConnection(socket));
	});

	// The socket grants full agent control: umask around listen() creates it
	// 0600 with no race window; chmod covers filesystems that ignore umask.
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
			const exitCode = signal === "SIGHUP" ? 129 : 143;
			// Route through RpcServer.shutdown so runtimeHost.dispose() runs.
			void socketServer?.server.shutdown(exitCode);
			// Never let a hung dispose trap the process on a signal.
			setTimeout(() => process.exit(exitCode), 5000).unref();
		});
	}

	// Keep process alive forever
	return new Promise(() => {});
}
