/**
 * Run modes for the coding agent.
 */

export { type AttachModeOptions, runAttachMode } from "./attach/attach-mode.ts";
export { RemoteAgentSession, type RemoteAgentSessionOptions } from "./attach/remote-agent-session.ts";
export {
	asAgentSessionRuntime,
	RemoteAgentSessionRuntime,
	type RemoteRuntimeOptions,
} from "./attach/remote-runtime.ts";
export {
	InteractiveMode,
	type InteractiveModeOptions,
	type SessionPickerHooks,
} from "./interactive/interactive-mode.ts";
export { type PrintModeOptions, runPrintMode } from "./print-mode.ts";
export {
	ModelInfo,
	RpcClient,
	type RpcClientOptions,
	type RpcCloseListener,
	type RpcEventListener,
} from "./rpc/rpc-client.ts";
export { runRpcMode } from "./rpc/rpc-mode.ts";
export { type RpcConnection, RpcServer, type RpcServerOptions } from "./rpc/rpc-server.ts";
export { type RpcSocketModeOptions, runRpcSocketMode } from "./rpc/rpc-socket-mode.ts";
export type {
	RpcCommand,
	RpcExtensionUIRequest,
	RpcExtensionUIResponse,
	RpcHello,
	RpcResponse,
	RpcSessionInfo,
	RpcSessionState,
} from "./rpc/rpc-types.ts";
export { RPC_CAPABILITIES, RPC_PROTOCOL_VERSION } from "./rpc/rpc-types.ts";
