export interface SchedulableWorker {
  id: string;
  eligible: boolean;
  secondsPerImage: number;
  availableInSeconds: number;
  capabilities: ReadonlySet<string>;
}

export interface SchedulableJob { id: string; profile: string; costFactor: number }
export interface Assignment { jobId: string; workerId: string; predictedDurationMs: number; finishesInSeconds: number }

export function assignJobs(workers: readonly SchedulableWorker[], jobs: readonly SchedulableJob[], tieEpsilonSeconds = 0): Assignment[] {
  const state = workers.map((worker) => ({ ...worker, virtualFinish: Math.max(0, worker.availableInSeconds), virtualJobs: 0 }));
  const assignments: Assignment[] = [];
  for (const job of jobs) {
    const candidates = state.filter((worker) => worker.eligible && worker.capabilities.has(job.profile));
    if (candidates.length === 0) continue;
    candidates.sort((left, right) => {
      const leftDuration = left.secondsPerImage * job.costFactor;
      const rightDuration = right.secondsPerImage * job.costFactor;
      const finishDifference = left.virtualFinish + leftDuration - (right.virtualFinish + rightDuration);
      if (Math.abs(finishDifference) > tieEpsilonSeconds) return finishDifference;
      if (Math.abs(leftDuration - rightDuration) > 1e-9) return leftDuration - rightDuration;
      if (left.virtualJobs !== right.virtualJobs) return left.virtualJobs - right.virtualJobs;
      return left.id.localeCompare(right.id);
    });
    const chosen = candidates[0];
    if (!chosen) continue;
    const duration = chosen.secondsPerImage * job.costFactor;
    chosen.virtualFinish += duration;
    chosen.virtualJobs += 1;
    assignments.push({ jobId: job.id, workerId: chosen.id, predictedDurationMs: Math.round(duration * 1000), finishesInSeconds: chosen.virtualFinish });
  }
  return assignments;
}

export function profileAndCost(envelope: Record<string, unknown>): { profile: string; costFactor: number } {
  const extra = envelope.extra_data;
  if (extra && typeof extra === "object" && !Array.isArray(extra)) {
    const gateway = (extra as Record<string, unknown>).gateway;
    if (gateway && typeof gateway === "object" && !Array.isArray(gateway)) {
      const profile = (gateway as Record<string, unknown>).profile;
      const factor = (gateway as Record<string, unknown>).cost_factor;
      return {
        profile: typeof profile === "string" && profile !== "" ? profile : "default",
        costFactor: typeof factor === "number" && Number.isFinite(factor) && factor > 0 && factor <= 100 ? factor : 1,
      };
    }
  }
  return { profile: "default", costFactor: 1 };
}
