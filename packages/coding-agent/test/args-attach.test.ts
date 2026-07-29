import { describe, expect, it } from "vitest";
import { parseArgs } from "../src/cli/args.ts";

describe("attach subcommand parsing", () => {
	it("parses --cmd", () => {
		const args = parseArgs(["attach", "--cmd", "docker exec -it sandbox pi --mode rpc"]);
		expect(args.attach).toEqual({ command: "docker exec -it sandbox pi --mode rpc" });
		expect(args.diagnostics).toEqual([]);
	});

	it("parses --sock", () => {
		const args = parseArgs(["attach", "--sock", "/tmp/agent.sock"]);
		expect(args.attach).toEqual({ sock: "/tmp/agent.sock" });
		expect(args.diagnostics).toEqual([]);
	});

	it("requires a transport", () => {
		const args = parseArgs(["attach"]);
		expect(args.diagnostics.some((d) => d.message.includes("requires --cmd"))).toBe(true);
	});

	it("rejects both transports", () => {
		const args = parseArgs(["attach", "--cmd", "x", "--sock", "/tmp/s"]);
		expect(args.diagnostics.some((d) => d.message.includes("mutually exclusive"))).toBe(true);
	});

	it("rejects unknown flags", () => {
		const args = parseArgs(["attach", "--sock", "/tmp/s", "--bogus"]);
		expect(args.diagnostics.some((d) => d.message.includes("Unknown argument"))).toBe(true);
	});

	it("does not treat attach elsewhere as subcommand", () => {
		const args = parseArgs(["--mode", "rpc"]);
		expect(args.attach).toBeUndefined();
	});
});
