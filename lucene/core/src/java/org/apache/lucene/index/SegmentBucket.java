package org.apache.lucene.index;

import java.util.Objects;

/**
 * Segment bucket class.
 */
public class SegmentBucket {
    private int bucket;

    /**
     * Constructor
     * @param bucket param
     */
    public SegmentBucket(int bucket) {
        this.bucket = bucket;
    }

    @Override
    public String toString() {
        return Integer.toString(bucket);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentBucket that = (SegmentBucket) o;
        return bucket == that.bucket;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket);
    }

    /**
     * bucket one
     */
    public static final SegmentBucket ONE = new SegmentBucket(1);

    /**
     * Default bucket
     */
    public static final SegmentBucket DEFAULT = new SegmentBucket(0);
}
