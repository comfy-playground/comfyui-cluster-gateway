import type { JsonObject } from "../types.js";

function object(value: unknown): JsonObject | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : undefined;
}

function rewritePromptIds(value: unknown, executionId: string, jobId: string): void {
  if (Array.isArray(value)) {
    for (const item of value) rewritePromptIds(item, executionId, jobId);
    return;
  }
  const valueObject = object(value);
  if (!valueObject) return;
  for (const [key, child] of Object.entries(valueObject)) {
    if (key === "prompt_id" && child === executionId) valueObject[key] = jobId;
    else rewritePromptIds(child, executionId, jobId);
  }
}

export function splitIndexedImageHistory(
  history: JsonObject,
  executionId: string,
  jobId: string,
  memberIndex: number,
  outputNodeIds?: readonly string[],
): JsonObject {
  const entry = object(history[executionId]);
  if (!entry) throw new Error("terminal history has no execution entry");
  const memberEntry = structuredClone(entry) as JsonObject;
  rewritePromptIds(memberEntry, executionId, jobId);
  const outputs = object(memberEntry.outputs);
  if (outputNodeIds && !outputs) throw new Error("batch history has no outputs for the declared plan");
  if (outputs) {
    const selectedNodes = outputNodeIds ? new Set(outputNodeIds) : undefined;
    const selectedOutputs: JsonObject = {};
    if (outputNodeIds) {
      for (const nodeId of outputNodeIds) {
        const output = object(outputs[nodeId]);
        if (!output || !Array.isArray(output.images) || !object(output.images[memberIndex])) {
          throw new Error(`batch output node ${nodeId} has no image for member ${memberIndex}`);
        }
      }
    }
    for (const [nodeId, outputValue] of Object.entries(outputs)) {
      if (selectedNodes && !selectedNodes.has(nodeId)) continue;
      const output = object(outputValue);
      if (!output || !Array.isArray(output.images)) continue;
      const image = output.images[memberIndex];
      output.images = image === undefined ? [] : [image];
      selectedOutputs[nodeId] = output as JsonObject;
    }
    memberEntry.outputs = selectedOutputs;
  }
  return { [jobId]: memberEntry } as JsonObject;
}
