import { expect, test } from "bun:test";

// Tests that fetch() with a streaming ReadableStream body correctly sends
// all data. This exercises the writeToStreamUsingBuffer path in http.zig.
test("fetch with ReadableStream body sends all chunks correctly", async () => {
  using server = Bun.serve({
    port: 0,
    async fetch(req) {
      // Echo the request body back
      const body = await req.text();
      return new Response(body);
    },
  });

  const totalChunks = 64;
  const chunkSize = 1024;
  let chunksEnqueued = 0;

  const stream = new ReadableStream({
    pull(controller) {
      if (chunksEnqueued >= totalChunks) {
        controller.close();
        return;
      }
      // Use a recognizable pattern so we can verify ordering
      const chunk = Buffer.alloc(chunkSize, chunksEnqueued % 256);
      controller.enqueue(chunk);
      chunksEnqueued++;
    },
  });

  const response = await fetch(server.url, {
    method: "POST",
    body: stream,
    duplex: "half",
  } as RequestInit);

  const result = new Uint8Array(await response.arrayBuffer());
  expect(result.length).toBe(totalChunks * chunkSize);

  // Verify ordering: each 1024-byte chunk should contain the same byte
  for (let i = 0; i < totalChunks; i++) {
    const expected = i % 256;
    const offset = i * chunkSize;
    expect(result[offset]).toBe(expected);
    expect(result[offset + chunkSize - 1]).toBe(expected);
  }
});

// Test with many small rapid chunks to increase likelihood of hitting
// the write2 fast path (buffered + new data simultaneously)
test("fetch with many small rapid stream chunks", async () => {
  using server = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = await req.bytes();
      return new Response(String(body.length));
    },
  });

  const totalChunks = 512;
  const chunkSize = 64;
  let chunksEnqueued = 0;

  const stream = new ReadableStream({
    pull(controller) {
      if (chunksEnqueued >= totalChunks) {
        controller.close();
        return;
      }
      controller.enqueue(new Uint8Array(chunkSize).fill(0xab));
      chunksEnqueued++;
    },
  });

  const response = await fetch(server.url, {
    method: "POST",
    body: stream,
    duplex: "half",
  } as RequestInit);

  const text = await response.text();
  expect(Number(text)).toBe(totalChunks * chunkSize);
});

// Test with a single large chunk
test("fetch with large ReadableStream chunk", async () => {
  using server = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = await req.bytes();
      return new Response(String(body.length));
    },
  });

  const size = 1024 * 1024; // 1MB
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(Buffer.alloc(size, 0x42));
      controller.close();
    },
  });

  const response = await fetch(server.url, {
    method: "POST",
    body: stream,
    duplex: "half",
  } as RequestInit);

  const text = await response.text();
  expect(Number(text)).toBe(size);
});
