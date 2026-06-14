import { ReadStream } from 'node:fs';
import type { ReadableStream } from 'node:stream/web';
import { inject, injectable } from 'tsyringe';

import { StorageBackend } from './storage-backend/storage-backend';
import { StorageBackendToken } from '../ioc-tokens';
import { computeChecksum } from './helpers';

export interface FileStorage {
    /**
     * Upload file should handle a web standards ReadableStream and put the file into the storage backend.
     * 
     * Chunk size is a parameter that should be used to determine the size of "chunks" of the file to store in
     * the storagebackend.
     *
     * Note: parallel is a "bonus" feature that should control the number of parallel requests made to the
     * storage backend
     */
    uploadFile(
        fileStream: ReadableStream<Uint8Array> | ReadStream,
        fileName: string,
        chunkSize: number,
        _parallel?: number
    ): Promise<void>;

    /**
     * Download file should return the full file that was uploaded by the given `fileName` as a Buffer.
     *
     * Note: parallel is a "bonus" feature that should control the number of parallel requests made to the
     * storage backend
     */
    downloadFile(fileName: string, _parallel?: number): Promise<Buffer>;

    /**
     * List uploaded files is primarily used in unit tests and would be a method for debugging. Therefore, it
     * does not need to be highly performant (for example might use `SCAN` or `KEYS` with a redis implementation).
     */
    listUploadedFiles(): Promise<string[]>;
}


/**
 * Key schema:
 *   {fileName}:chunk:{index}       — Buffer containing the raw bytes of that chunk
 *   {fileName}:checksum:{index}    — hex SHA-1 of that chunk (integrity verification on download)
 *   {fileName}:meta:chunkCount     — string containing total number of chunks
 */
function chunkKey(fileName: string, index: number): string {
    return `${fileName}:chunk:${index}`;
}

function checksumKey(fileName: string, index: number): string {
    return `${fileName}:checksum:${index}`;
}

function metaChunkCountKey(fileName: string): string {
    return `${fileName}:meta:chunkCount`;
}

/**
 * Collect a ReadableStream<Uint8Array> or Node ReadStream into fixed-size byte chunks.
 * Each yielded chunk is exactly `chunkSize` bytes, except possibly the last one.
 */
async function* toChunks(
    stream: ReadableStream<Uint8Array> | ReadStream,
    chunkSize: number
): AsyncGenerator<Buffer> {
    let accumulated = Buffer.alloc(0);

    // Normalise to an async iterable
    const iterable: AsyncIterable<Uint8Array> =
        Symbol.asyncIterator in stream
            ? (stream as AsyncIterable<Uint8Array>)
            : (stream as ReadableStream<Uint8Array>)[Symbol.asyncIterator]
                ? (stream as AsyncIterable<Uint8Array>)
                : streamToAsyncIterable(stream as ReadableStream<Uint8Array>);

    for await (const incoming of iterable) {
        accumulated = Buffer.concat([accumulated, Buffer.from(incoming)]);

        while (accumulated.length >= chunkSize) {
            yield accumulated.subarray(0, chunkSize);
            accumulated = accumulated.subarray(chunkSize);
        }
    }

    if (accumulated.length > 0) {
        yield accumulated;
    }
}

/**
 * Adapt a Web Streams ReadableStream to an AsyncIterable so we can use `for await`.
 */
async function* streamToAsyncIterable(
    stream: ReadableStream<Uint8Array>
): AsyncGenerator<Uint8Array> {
    const reader = stream.getReader();
    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            yield value;
        }
    } finally {
        reader.releaseLock();
    }
}

@injectable()
export class AppFileStorage implements FileStorage {
    constructor(@inject(StorageBackendToken) private backend: StorageBackend) {
        console.log('TODO: implement AppFileStorage', this.backend);
    }

    /**
        * Splits `fileStream` into `chunkSize`-byte chunks, persists each chunk together with its
        * SHA-1 checksum, and writes a metadata entry that records how many chunks make up the file.
        *
        * The `parallel` parameter (bonus feature) controls the concurrency of the backend `set` calls
        * made per batch of chunks. Sequential upload is used when parallel is 1 (or omitted).
        */
    public async uploadFile(
        fileStream: ReadableStream<Uint8Array> | ReadStream,
        fileName: string,
        chunkSize: number,
        parallel: number = 1
    ): Promise<void> {
        let chunkIndex = 0;
        const pendingWrites: Promise<void>[] = [];

        const flushWrites = async (writes: Promise<void>[]): Promise<void> => {
            await Promise.all(writes);
        };

        for await (const chunk of toChunks(fileStream, chunkSize)) {
            const index = chunkIndex++;
            const checksum = computeChecksum(chunk);

            const writeChunk = async (): Promise<void> => {
                await this.backend.set(chunkKey(fileName, index), chunk);
                await this.backend.set(checksumKey(fileName, index), checksum);
            };

            pendingWrites.push(writeChunk());

            // Flush in batches of `parallel` to honour the concurrency limit
            if (pendingWrites.length >= parallel) {
                await flushWrites(pendingWrites.splice(0, parallel));
            }
        }

        // Flush any remaining writes
        if (pendingWrites.length > 0) {
            await flushWrites(pendingWrites);
        }

        // Record total chunk count so download can reconstruct without a KEYS scan
        await this.backend.set(metaChunkCountKey(fileName), String(chunkIndex));
    }

    /**
     * Reassembles the file from its stored chunks in order, verifying each chunk's SHA-1
     * checksum before including it in the output. Throws if the file is not found or if any
     * chunk fails its integrity check.
     *
     * The `parallel` parameter (bonus feature) controls the concurrency of the backend `getBuffer`
     * calls used to fetch chunks.
     */
    public async downloadFile(fileName: string, parallel: number = 1): Promise<Buffer> {
        const chunkCountStr = await this.backend.get(metaChunkCountKey(fileName));
        if (chunkCountStr === null) {
            throw new Error(`File ${fileName} not found`);
        }

        const chunkCount = parseInt(chunkCountStr, 10);
        const chunks: Buffer[] = new Array<Buffer>(chunkCount);

        // Fetch chunks in parallel batches
        for (let batchStart = 0; batchStart < chunkCount; batchStart += parallel) {
            const batchEnd = Math.min(batchStart + parallel, chunkCount);
            const batchIndices = Array.from(
                { length: batchEnd - batchStart },
                (_, i) => batchStart + i
            );

            const fetchedChunks = await Promise.all(
                batchIndices.map(async (index) => {
                    const [chunk, expectedChecksum] = await Promise.all([
                        this.backend.getBuffer(chunkKey(fileName, index)),
                        this.backend.get(checksumKey(fileName, index)),
                    ]);

                    if (chunk === null) {
                        throw new Error(`File ${fileName} not found: missing chunk ${index}`);
                    }

                    if (expectedChecksum === null) {
                        throw new Error(
                            `File ${fileName} not found: missing checksum for chunk ${index}`
                        );
                    }

                    const actualChecksum = computeChecksum(chunk);
                    if (actualChecksum !== expectedChecksum) {
                        throw new Error(
                            `File ${fileName} is corrupted: checksum mismatch on chunk ${index}`
                        );
                    }

                    return { index, chunk };
                })
            );

            for (const { index, chunk } of fetchedChunks) {
                chunks[index] = chunk;
            }
        }

        return Buffer.concat(chunks);
    }
    /**
     * Lists file names that have been uploaded. Uses the metadata key pattern so each file
     * appears exactly once (one meta entry per file), regardless of how many chunks it has.
     */
    public async listUploadedFiles(): Promise<string[]> {
        const metaKeys = await this.backend.keys('*:meta:chunkCount');
        return metaKeys.map((key) => key.replace(':meta:chunkCount', ''));
    }
}
