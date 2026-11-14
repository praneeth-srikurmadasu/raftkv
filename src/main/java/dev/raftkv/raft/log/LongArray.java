package dev.raftkv.raft.log;

import java.util.Arrays;

/** A growable array of primitive longs (avoids boxing for the log's in-memory index). */
final class LongArray {

    private long[] values = new long[64];
    private int size;

    void add(long v) {
        if (size == values.length) {
            values = Arrays.copyOf(values, size * 2);
        }
        values[size++] = v;
    }

    long get(int i) {
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException(i + " of " + size);
        }
        return values[i];
    }

    int size() {
        return size;
    }

    void truncate(int newSize) {
        if (newSize < 0 || newSize > size) {
            throw new IndexOutOfBoundsException(newSize + " of " + size);
        }
        size = newSize;
    }
}
