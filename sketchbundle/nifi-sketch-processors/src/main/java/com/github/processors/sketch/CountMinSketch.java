package com.github.processors.sketch;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/*
 * This Class implements probabilistic data structure 'Count-Min Sketch'
 * Ref: http://dimacs.rutgers.edu/~graham/pubs/papers/cmencyc.pdf
 * Ref: https://github.com/prasanthj/count-min-sketch
 * 
 * @author prashanth
 * */
public final class CountMinSketch {
	private static CountMinSketch instance = null;
	private final int width;
	private final int depth;
	private final long[][] sketch;
	private final HashFunction murmur_;

	public int getWidth() {
		return width;
	}

	public int getDepth() {
		return depth;
	}

	public long[][] getSketch() {
		return sketch;
	}

	private CountMinSketch(final float delta, final float epsilon) {
		this.width = (int) Math.ceil(Math.exp(1.0) / epsilon);
		this.depth = (int) Math.ceil(Math.log(1.0 / delta));
		this.sketch = new long[depth][width];
		this.murmur_ = Hashing.murmur3_128();
	}

	private CountMinSketch(final int width, final int depth) {
		this.width = width;
		this.depth = depth;
		this.sketch = new long[depth][width];
		this.murmur_ = Hashing.murmur3_128();
	}

	private CountMinSketch(final float delta, final float epsilon, final int seed) {
		this.width = (int) Math.ceil(Math.exp(1.0) / epsilon);
		this.depth = (int) Math.ceil(Math.log(1.0 / delta));
		this.sketch = new long[depth][width];
		this.murmur_ = Hashing.murmur3_128(seed);
	}

	private CountMinSketch(final int width, final int depth, final int seed) {
		this.width = width;
		this.depth = depth;
		this.sketch = new long[depth][width];
		this.murmur_ = Hashing.murmur3_128(seed);
	}

	public final static void createInstance(final float delta, final float epsilon) {
		if (null == instance) {
			synchronized (CountMinSketch.class) {
				instance = new CountMinSketch(delta, epsilon);
			}
		}
	}

	public final static void createInstance(final int width, final int depth) {
		if (null == instance) {
			synchronized (CountMinSketch.class) {
				instance = new CountMinSketch(width, depth);
			}
		}
	}

	public final static void createInstance(final float delta, final float epsilon, final int seed) {
		if (null == instance) {
			synchronized (CountMinSketch.class) {
				instance = new CountMinSketch(delta, epsilon, seed);
			}
		}
	}

	public final static void createInstance(final int width, final int depth, final int seed) {
		if (null == instance) {
			synchronized (CountMinSketch.class) {
				instance = new CountMinSketch(width, depth, seed);
			}
		}
	}

	public final static CountMinSketch getInstance() {
		assert null != instance : "Null Instane Found";
		return instance;
	}

	public void update(final String key) {
		update(key, 1);
	}

	public void update(final String key, final long value) {
		final long murmur_128 = murmur_.newHasher().putBytes(key.getBytes()).hash().asLong();
		int hash1 = (int) murmur_128;
		int hash2 = (int) (murmur_128 >>> 32);
		for (int i = 1; i <= depth; i++) {
			int uniqueHash = hash1 + (i * hash2);
			// hashcode should be positive, flip all the bits if it's negative
			if (uniqueHash < 0) {
				uniqueHash = ~uniqueHash;
			}
			int pos = uniqueHash % width;
			sketch[i - 1][pos] += value;
		}
	}

	public long getEstimatedCount(final String key) {
		final long murmur_128 = murmur_.newHasher().putBytes(key.getBytes()).hash().asLong();
		int hash1 = (int) murmur_128;
		int hash2 = (int) (murmur_128 >>> 32);
		long min = Long.MAX_VALUE;
		for (int i = 1; i <= depth; i++) {
			int uniqueHash = hash1 + (i * hash2);
			// hashcode should be positive, flip all the bits if it's negative
			if (uniqueHash < 0) {
				uniqueHash = ~uniqueHash;
			}
			int pos = uniqueHash % width;
			min = Math.min(min, sketch[i - 1][pos]);
		}
		return min;
	}
}
