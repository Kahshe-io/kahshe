package io.kahshe.indexer;

import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.FormatConfig;
/**
 * What a build needs beyond the format: the knobs that size and bound it. Carries the
 * {@link FormatConfig} because a build must know where the index lives and which tiers the
 * deployment allows; the reverse dependency does not exist, so a reader never sees these.
 *
 * @param maxTokenLength default cap on an indexed token; per-column overrides ride on table
 *     properties (see {@link Analyzer}). Changing it changes the analyzer id and needs a reindex.
 * @param ngram default gram size for the bloom and gram tiers; per-column overrides ride on table
 *     properties
 * @param fleet whether this process is one of several builders sharing the work. A fleet member
 *     SKIPS a column another member holds the lease on, counting it; a lone builder refuses
 *     loudly, because there nothing else is going to build it. See
 *     {@link io.kahshe.indexer.maintain.Fleet}.
 * @param fleetOrdinal this member's claim order; negative derives one from the hostname
 */
public record BuildConfig(
    FormatConfig format,
    int maxTokenLength,
    int ngram,
    long gramBuildMaxBytes,
    long termBuildMaxSpillBytes,
    String termBuildDir,
    int indexThreads,
    long termBufferBytes,
    long indexStaleWarnMs,
    boolean fleet,
    int fleetOrdinal) {

  /** The single-builder defaults, for a caller that has no fleet to configure. */
  public BuildConfig(
      FormatConfig format,
      int maxTokenLength,
      int ngram,
      long gramBuildMaxBytes,
      long termBuildMaxSpillBytes,
      String termBuildDir,
      int indexThreads,
      long termBufferBytes,
      long indexStaleWarnMs) {
    this(format, maxTokenLength, ngram, gramBuildMaxBytes, termBuildMaxSpillBytes, termBuildDir,
        indexThreads, termBufferBytes, indexStaleWarnMs, false, -1);
  }
}
