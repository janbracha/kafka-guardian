package cz.company.guardian.validation;

import java.util.Arrays;

/**
 * Immutable set of non-negative {@code int}s with O(1) lookup and no boxing.
 * Open addressing with a load factor of at most 0.5, so a probe sequence always ends in an empty slot.
 */
public final class SchemaIdSet {

    private static final int EMPTY = -1;

    private final int[] table;
    private final int mask;
    private final int size;

    private SchemaIdSet(int[] table, int size) {
        this.table = table;
        this.mask = table.length - 1;
        this.size = size;
    }

    /**
     * @param ids non-negative, possibly repeated schema IDs
     * @throws IllegalArgumentException if any id is negative
     */
    public static SchemaIdSet of(int... ids) {
        int capacity = 4;
        while (capacity < ids.length * 2) {
            capacity <<= 1;
        }
        int[] table = new int[capacity];
        Arrays.fill(table, EMPTY);
        int mask = capacity - 1;
        int size = 0;
        for (int id : ids) {
            if (id < 0) {
                throw new IllegalArgumentException("schema id must not be negative: " + id);
            }
            int slot = mix(id) & mask;
            while (table[slot] != EMPTY && table[slot] != id) {
                slot = (slot + 1) & mask;
            }
            if (table[slot] == EMPTY) {
                table[slot] = id;
                size++;
            }
        }
        return new SchemaIdSet(table, size);
    }

    public boolean contains(int id) {
        if (id < 0) {
            return false;
        }
        int slot = mix(id) & mask;
        int candidate;
        while ((candidate = table[slot]) != EMPTY) {
            if (candidate == id) {
                return true;
            }
            slot = (slot + 1) & mask;
        }
        return false;
    }

    public int size() {
        return size;
    }

    /** Sorted copy of the contents; for diagnostics and tests, not for the hot path. */
    public int[] toSortedArray() {
        return Arrays.stream(table).filter(v -> v != EMPTY).sorted().toArray();
    }

    @Override
    public String toString() {
        return Arrays.toString(toSortedArray());
    }

    private static int mix(int x) {
        int h = x * 0x9E3779B1;
        return h ^ (h >>> 16);
    }
}
