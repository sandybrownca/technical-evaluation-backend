import { injectable } from 'tsyringe';
import { computeChecksum } from '../helpers';
import { StorageBackend, StoredValue } from './storage-backend';

@injectable()
export class TestStorageBackend implements StorageBackend {
    private store: Record<string, StoredValue> = {};

    private listStore: Record<string, StoredValue[]> = {};

    constructor() {}

    async verifyChecksum(key: string): Promise<string> {
        const value = this.store[key];
        if (!value) {
            throw new Error(`Key not found: ${key}`);
        }

        // Simulate checksum calculation (e.g., SHA-1)
        const data = typeof value === 'string' ? Buffer.from(value) : value;
        const checksum = computeChecksum(data);
        return checksum;
    }

    async get(key: string): Promise<string | null> {
        const value = this.store[key];
        return typeof value === 'string' ? value : null;
    }

    async getBuffer(key: string): Promise<Buffer | null> {
        const value = this.store[key];
        return Buffer.isBuffer(value) ? value : null;
    }

    async getListAll(key: string): Promise<string[]> {
        const retrievedList = this.listStore[key] || [];
        return retrievedList.map((storedVal) => {
            if (storedVal instanceof Buffer) {
                return storedVal.toString('utf8');
            }
            return storedVal;
        });
    }

    async set(key: string, value: StoredValue): Promise<void> {
        this.store[key] = value;
    }

    /**
     * Fixed: the original implementation wrapped the push in setTimeout, causing the mutation
     * to happen after the promise resolved. The async function now performs the push synchronously
     * within the same microtask, so awaiting rPush correctly reflects the updated list.
     */
    async rPush(key: string, value: StoredValue): Promise<void> {
        const existingList = this.listStore[key] || [];
        this.listStore[key] = [...existingList, value];
    }

    async keys(pattern: string): Promise<string[]> {
        const regex = new RegExp(`^${pattern.replace('*', '.*')}$`);
        return Object.keys(this.store).filter((key) => regex.test(key));
    }
}
