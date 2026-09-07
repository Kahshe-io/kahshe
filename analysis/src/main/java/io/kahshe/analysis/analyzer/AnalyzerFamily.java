package io.kahshe.analysis.analyzer;

import java.util.List;

/**
 * One analyzer family: the owner of a region of the analyzer-id space, and the whole of what an
 * index built under one of its ids means. Implementations are discovered by
 * {@link java.util.ServiceLoader}; {@link AsciiAnalyzerFamily} and {@link ValueAnalyzerFamily} are
 * the built-ins.
 *
 * <p>What an implementation must honour: {@link #owns} is a pure test on the id text alone and is
 * false for every id another family owns; {@link #parse} is called only for an owned id and
 * returns a {@link Analyzer.Contract} carrying this family; {@link #id} is its inverse, so
 * {@code parse(id).id().equals(id)}. Build and probe tokenize through the same contract, so a
 * family that answers differently for one id in two processes desynchronises every index it wrote.
 * An id no loaded family owns is refused by the reader, which keeps every file and serves unpruned
 * rather than misreading it.
 */
public interface AnalyzerFamily {
  /** A short, stable name for this family; used in logs and to say which families are loaded. */
  String name();

  /** What this family makes of a value: ASCII-style tokens, or the whole value. */
  Analyzer.Kind kind();

  /** Whether {@code id} names a contract of this family. Pure, and false for a null id. */
  boolean owns(String id);

  /** The contract {@code id} names. Called only when {@link #owns} said yes. */
  Analyzer.Contract parse(String id);

  /** The terms a value contributes to the index under {@code contract}. */
  List<String> tokens(Analyzer.Contract contract, String text);

  /** The terms a query probes for under {@code contract}; usually {@link #tokens}. */
  List<String> queryTerms(Analyzer.Contract contract, String text);

  /** Whether {@code token} is admitted to the dictionary under {@code contract}. */
  boolean isIndexable(Analyzer.Contract contract, String token);

  /** Whether {@code prefix} can start a term this contract writes; see {@link Analyzer.Contract#prefixable}. */
  boolean prefixable(Analyzer.Contract contract, String prefix);

  /** The id an index built under {@code contract} records; the inverse of {@link #parse}. */
  String id(Analyzer.Contract contract);
}
