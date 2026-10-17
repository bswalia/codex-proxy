/** Forward each upstream byte chunk on demand and cancel upstream if the client goes away. */
export function relayUpstreamStream(
  upstream: ReadableStream<Uint8Array>,
  upstreamAbort: AbortController,
): ReadableStream<Uint8Array> {
  const reader = upstream.getReader()
  let finished = false

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read()
        if (done) {
          finished = true
          reader.releaseLock()
          controller.close()
          return
        }
        controller.enqueue(value)
      } catch (error) {
        if (!finished) {
          finished = true
          reader.releaseLock()
        }
        controller.error(error)
      }
    },
    async cancel(reason) {
      if (finished) return
      finished = true
      upstreamAbort.abort(reason)
      try {
        await reader.cancel(reason)
      } finally {
        reader.releaseLock()
      }
    },
  })
}

export async function collectStream(stream: ReadableStream<Uint8Array>): Promise<string | null> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let buffer = ""
  let collected = ""

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })

      const lines = buffer.split("\n")
      buffer = lines.pop() ?? ""

      for (const line of lines) {
        if (!line.startsWith("data: ")) continue
        const payload = line.slice(6).trim()
        if (payload === "[DONE]") continue
        try {
          const chunk = JSON.parse(payload)
          const delta = chunk?.choices?.[0]?.delta?.content
          if (delta) collected += delta
          if (chunk?.type === "response.output_text.delta" && chunk?.delta) {
            collected += chunk.delta
          }
        } catch {}
      }
    }
  } catch {} finally {
    reader.releaseLock()
  }

  return collected || null
}
