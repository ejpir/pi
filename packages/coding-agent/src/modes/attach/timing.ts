/**
 * Attach-phase timing instrumentation (PI_ATTACH_TIMING=1 or --verbose).
 *
 * Re-attach cost decomposes into: transport spawn, hello handshake,
 * refetchAll (per-RPC latency + payload size), TUI init. When a user
 * reports "re-attach takes forever", these stamps show which term grew —
 * the alternative is guessing. Lines append to ~/.pi/attach-timing.log so
 * they can be tailed live without corrupting the TUI; pre-TUI phases also
 * go to stderr under --verbose.
 */

import { appendFileSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface AttachTiming {
	phase: string;
	ms: number;
	detail?: string;
}

const timings: AttachTiming[] = [];
let enabled = false;
let startTime = 0;

export function enableAttachTiming(): void {
	enabled = true;
	startTime = performance.now();
}

export function attachTimingEnabled(): boolean {
	return enabled;
}

/** Record a phase completion. Returns nothing; call with the phase's start timestamp. */
export function recordTiming(phase: string, phaseStart: number, detail?: string): void {
	if (!enabled) return;
	const entry: AttachTiming = { phase, ms: performance.now() - phaseStart, detail };
	timings.push(entry);
	const line = `${new Date().toISOString()} +${((performance.now() - startTime) / 1000).toFixed(2)}s ${phase}: ${entry.ms.toFixed(1)}ms${detail ? ` (${detail})` : ""}`;
	try {
		const dir = join(homedir(), ".pi");
		mkdirSync(dir, { recursive: true });
		appendFileSync(join(dir, "attach-timing.log"), `${line}\n`);
	} catch {
		// Timing must never break an attach.
	}
}

export function getAttachTimings(): readonly AttachTiming[] {
	return timings;
}
