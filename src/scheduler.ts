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
