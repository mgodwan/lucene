package org.apache.lucene.index;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Segment bucket class.
 */
public class SegmentBucket {
    private String bucket;

    /**
     * Constructor
     * @param bucket param
     */
    public SegmentBucket(String bucket) {
        this.bucket = bucket;
    }

    @Override
    public String toString() {
        return bucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentBucket that = (SegmentBucket) o;
        return bucket.equals(that.bucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket);
    }

    private static Map<String, SegmentBucket> BUCKETS = new ConcurrentHashMap<>();

    public static SegmentBucket getBucket(String id) {
        return BUCKETS.computeIfAbsent(id, SegmentBucket::new);
    }

    public static final SegmentBucket DEFAULT = new SegmentBucket("-1");
}
