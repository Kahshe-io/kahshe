package io.kahshe.watch.scan;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.format.type.IndexType;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexType;
import io.kahshe.watch.rules.Conditions;
import io.kahshe.watch.rules.ConfirmationSql;
import io.kahshe.watch.rules.WatchRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.roaringbitmap.RoaringBitmap;

/**
 * One term, or one rule of the shape the index can answer, evaluated over EVERY data file a table
 * holds, from the term index alone — the retroactive question the prospective paths cannot ask:
 * "were we already hit?"
 *
 * <p>The index already knows the answer for the whole table, not only for files that landed after
 * a rule did. What it does not do is decide what ignorance means. {@code IndexPruner.prune} keeps a
 * file the index cannot speak for — one with no ordinal in the coverage list — because for a scan
 * the safe reading of ignorance is "read it", and its result is {@code kept = hit ∪ unresolved}
 * with nothing to tell the two apart. For a hunt that file must be reported as UNRESOLVED: reading
 * it as a hit fabricates evidence, reading it as a miss declares clean what was never examined. So
 * this class does not call the pruner.
 *
 * <p>It does, since 2026-09-10, call the same VERDICT RULE the pruner's term tier calls:
 * {@link TermIndexType#partitionByCoverage} says covered-and-named is a hit, covered-and-not is a
 * proven absence, and uncovered is unknown — and it says it once, for both readers. Until then this class resolved each ordinal and probed each bitmap itself, which is
 * two copies of a rule that has exactly one safe reading. What stays here is what is genuinely a
 * hunt's and not the index's: the live-file enumeration below (the table's own list, never the
 * coverage list — coverage is the thing being checked), the refusals by name, the rule shape, and
 * the decision that unknown means "must be scanned to be decided".
 *
 * <p>The division that split is worth keeping in mind when changing either side: the FETCH belongs
 * to each caller, because they disagree about failure — the pruner swallows an unreadable leaf and
 * keeps every file, a hunt refuses by name — while the READING of what was fetched belongs to the
 * index. {@code HuntPassTest.hitDivergesFromThePrunerWhenCoverageIsPartial} pins that the two
 * still diverge exactly where coverage ends, which is now a property of one mechanism used two
 * ways rather than of two mechanisms agreeing.
 *
 * <p>A rule is admitted only in the shape {@link WatchRule#ridesIndex} names — one column, token
 * fields, one field or {@code any-of} over several — and the reason for a refusal is the one that
 * method gives, so the two cannot disagree about what the index can answer. Two more shapes that
 * ride a BUILD do not ride a hunt: {@code contains}, because the gram tier's verdict is advisory
 * and on identifier-dense text it prunes nothing; and {@code min_count} above one, because the
 * dictionary records which files hold a term and not how many times each. Each file's verdict
 * goes through {@link Conditions#eval}, the same interpreter the watcher's two paths use.
 *
 * <p>It refuses, by name, in every case where the pruner would silently keep every file: an
 * unknown column, a column with no term index, a value that is not one token the index's own
 * analyzer admits, an unreadable leaf. The pruner's keep-everything costs a slower scan; a hunt's
 * would report every file in the table as a hit.
 *
 * <p>It never alerts. A hunt over history claiming (rule, file) through
 * {@link io.kahshe.watch.Alerts} would silence the live watcher on every file it touched. Its
 * output is a result set — {@link Result#writeJsonl} — carrying, when asked, the SQL an
 * operator runs to see the rows ({@link Result#withConfirmationSql}): over the hits and the
 * unresolved files, never a miss, and never executed here.
 *
 * <p>On the port boundary with {@link ScanPass}, and for the same reason: this is the reader that
 * enumerates a table's files, so it names the table format; the verdict it feeds does not.
 */
public final class HuntPass {

  /** The hunt cannot be answered from the index without approximating, and says why. */
  public static final class Refused extends RuntimeException {
    public Refused(String message) {
      super(message);
    }

    public Refused(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * The table's live data files, sorted three ways, and what was asked of them. Every live file
   * is in exactly one list, and each list is in path order.
   *
   * @param table the table's qualified name as the catalog gives it
   * @param column the column the terms were looked up in
   * @param terms the tokens probed, under the index's analyzer, in field order
   * @param rule the rule's id when a rule was hunted; null for a single term
   * @param fields the fields as asked — one MATCH field for a single term, the rule's own
   *     otherwise — kept so the confirmation SQL asks the same question the result answered
   * @param expr the condition over {@code fields}, the tree {@link Conditions#eval} decided with
   * @param confirmationSql the query that shows the rows, or null until
   *     {@link #withConfirmationSql} sets it — and null after it when no file can hold a row
   * @param hit covered by the index, and the condition holds of the file's term bitmaps: the
   *     rule's tokens are in it, exact at file granularity
   * @param miss covered, and the condition does not hold: provably absent. Never worth scanning
   * @param unresolved outside the index's coverage: not examined, so neither a hit nor a miss. The
   *     only honest verdict is "must be scanned to be decided"
   * @param snapshotId the table snapshot the file list was taken at
   * @param indexSnapshotId the snapshot the index was built at. When the two differ, every file
   *     added since is unresolved — the gap is reported, never hidden
   * @param analyzer the index's analyzer id, whose admission rule decided whether the terms could
   *     be probed at all
   * @param countsExact whether an occurrence count read off this index would be exact
   *     ({@code counts-exact}, FORMAT.md §5.6); a consumer that prints counts must refuse when
   *     this is false
   * @param partial whether the index is a checkpoint over part of the build's files (§5.7)
   * @param confidence {@code exact} on a snapshot PROVEN to carry no delete files, {@code advisory}
   *     otherwise — the same rule the watcher's two paths apply. The index reads raw data files and
   *     applies no delete, so on a merge-on-read snapshot a hit file may hold only rows the engine
   *     no longer returns, and the confirmation SQL, which does apply the deletes, may return none
   * @param occurrences per probed token, how many times it occurs across every covered file — the
   *     dictionary's {@code total_count} — present only when the index's counts are exact
   *     ({@code counts-exact}, FORMAT.md §5.6); absent rather than an upper bound labelled a count
   */
  public record Result(
      String table,
      String column,
      List<String> terms,
      String rule,
      List<WatchRule.Field> fields,
      WatchRule.Expr expr,
      String confirmationSql,
      List<String> hit,
      List<String> miss,
      List<String> unresolved,
      long snapshotId,
      long indexSnapshotId,
      String analyzer,
      boolean countsExact,
      boolean partial,
      String confidence,
      Map<String, Long> occurrences) {
    public Result {
      terms = List.copyOf(terms);
      occurrences = occurrences == null ? null : Map.copyOf(occurrences);
      fields = List.copyOf(fields);
      hit = List.copyOf(hit);
      miss = List.copyOf(miss);
      unresolved = List.copyOf(unresolved);
    }

    /**
     * This partition with the confirmation SQL set: the condition as asked, on this snapshot,
     * over the hits and the unresolved files — the only files a matching row can be in. A miss is
     * never in it; the dictionary proved it cannot match. No hit and no unresolved file means no
     * row to confirm, and the SQL stays null rather than asking the whole table for nothing.
     *
     * <p>Only token fields reach a hunt, and a token compiles to {@code position(...)} on any
     * column type, so no column is numeric for the compiler's purposes.
     *
     * @param catalog the engine's catalog name, {@code KAHSHE_WATCH_SQL_CATALOG}
     * @param namespace the table's namespace as the engine addresses it
     * @param tableName the table's name as the engine addresses it
     */
    public Result withConfirmationSql(String catalog, String namespace, String tableName) {
      List<String> candidates = new ArrayList<>(hit);
      candidates.addAll(unresolved);
      candidates.sort(null);
      String sql = candidates.isEmpty()
          ? null
          : ConfirmationSql.hunt(
              catalog, namespace, tableName, snapshotId, candidates, fields, Set.of(), expr);
      return new Result(table, column, terms, rule, fields, expr, sql, hit, miss, unresolved,
          snapshotId, indexSnapshotId, analyzer, countsExact, partial, confidence, occurrences);
    }

    /**
     * The shape {@code kahshe hunt} prints. What was asked, counts for all three lists, the
     * confirmation SQL when set, and paths for the two lists an analyst acts on — the hits, and
     * the files that still need a scan. A miss needs nothing, and on a table the size this is
     * for, listing 219 of them would bury the one that does.
     */
    public ObjectNode toJson() {
      ObjectNode node = JsonNodeFactory.instance.objectNode();
      node.put("table", table);
      node.put("column", column);
      ArrayNode probed = node.putArray("terms");
      terms.forEach(probed::add);
      if (rule != null) {
        node.put("rule", rule);
      }
      node.put("snapshot_id", snapshotId);
      node.put("index_snapshot_id", indexSnapshotId);
      node.put("analyzer", analyzer);
      node.put("counts_exact", countsExact);
      node.put("partial", partial);
      node.put("confidence", confidence);
      if (occurrences != null) {
        ObjectNode counts = node.putObject("occurrences");
        occurrences.forEach(counts::put);
      }
      ObjectNode files = node.putObject("files");
      files.put("total", hit.size() + miss.size() + unresolved.size());
      files.put("hit", hit.size());
      files.put("miss", miss.size());
      files.put("unresolved", unresolved.size());
      // Said in a word, because the number is what a reader skims past: an unresolved count of
      // zero is the only case in which "no hits" means "not there".
      files.put("coverage", unresolved.isEmpty() ? "complete" : "partial");
      if (confirmationSql != null) {
        node.put("confirmation_sql", confirmationSql);
      }
      ArrayNode hits = node.putArray("hit");
      hit.forEach(hits::add);
      ArrayNode open = node.putArray("unresolved");
      unresolved.forEach(open::add);
      return node;
    }

    /**
     * The result set, as JSON lines: one {@code summary} record — {@link #toJson} without the
     * path arrays — then one {@code file} record per hit and per unresolved file, each with its
     * verdict. A miss is counted in the summary and not listed, for the reason {@link #toJson}
     * gives. This is a file an analyst keeps and a script reads; it is not an alert and goes
     * through no sink.
     */
    public void writeJsonl(Appendable out) throws IOException {
      ObjectNode summary = JsonNodeFactory.instance.objectNode();
      summary.put("kind", "summary");
      summary.setAll(toJson());
      summary.remove("hit");
      summary.remove("unresolved");
      out.append(summary.toString()).append('\n');
      for (String path : hit) {
        out.append(fileLine("hit", path)).append('\n');
      }
      for (String path : unresolved) {
        out.append(fileLine("unresolved", path)).append('\n');
      }
    }

    private static String fileLine(String verdict, String path) {
      ObjectNode line = JsonNodeFactory.instance.objectNode();
      line.put("kind", "file");
      line.put("verdict", verdict);
      line.put("path", path);
      return line.toString();
    }
  }

  private final TermIndex termIndex;

  public HuntPass(TermIndex termIndex) {
    this.termIndex = termIndex;
  }

  /**
   * Results the table's live data files by whether {@code value}, as one term under the
   * index's analyzer, is in each of them.
   *
   * @throws Refused rather than answering approximately — see the class comment for the cases
   */
  public Result hunt(Table table, String column, String value) {
    return lookup(
        table, column,
        List.of(new WatchRule.Field(column, WatchRule.Op.MATCH, List.of(value))),
        new WatchRule.Expr.FieldRef(0), null);
  }

  /**
   * Results the table's live data files by whether {@code rule}'s condition holds of each —
   * one verdict per field from that field's tokens' bitmaps, joined by the rule's own tree.
   *
   * @throws Refused for any shape the index cannot answer exactly, naming it: the reason
   *     {@link WatchRule#whyNotRidesIndex} gives, or {@code contains}, or {@code min_count}
   */
  public Result hunt(Table table, WatchRule rule) {
    String why = rule.whyNotRidesIndex();
    if (why != null) {
      throw new Refused("rule " + rule.id() + " cannot be answered from the index alone: " + why);
    }
    // Admitted to a build, where the file is being read anyway; not to a hunt. The gram tier says
    // a file holds every gram of the literal, which strongly suggests and does not prove the
    // substring, and on identifier-dense text every file holds every gram.
    if (!rule.contains().isEmpty()) {
      throw new Refused(
          "rule " + rule.id() + " uses contains: the gram tier's verdict is advisory, not a hit, "
              + "and a contains hunt is not built");
    }
    // The aggregate tier holds which files carry a term. The per-file count a build hands the
    // watcher is not in it, so a threshold over it cannot be answered.
    if (rule.minCount() > 1) {
      throw new Refused(
          "rule " + rule.id() + " sets min_count " + rule.minCount()
              + ": the dictionary records which files hold a term, not how many times each");
    }
    return lookup(table, rule.column(), rule.where(), rule.expr(), rule.id());
  }

  /**
   * The one loop both entry points share, so the three verdicts are decided in one place. Each
   * field is satisfied by a file when ANY of its values' bitmaps holds the file's ordinal — a
   * field's values are OR'ed by the rule's contract — and {@code expr} joins the fields.
   */
  private Result lookup(
      Table table, String column, List<WatchRule.Field> fields, WatchRule.Expr expr,
      String ruleId) {
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      throw new Refused("table has no snapshot: nothing to hunt");
    }
    // Resolved here, not left to the reader: the plan path keeps every file for a column it cannot
    // find, and a hunt that inherited that would report "in every file" for a typo.
    Types.NestedField field = table.schema().findField(column);
    if (field == null) {
      throw new Refused("no such column: " + column);
    }
    TermIndex.Loaded index = termIndex.forField(table, field.fieldId());
    if (index == null) {
      throw new Refused(
          "no term index for column " + column
              + ": only the term dictionary can answer over every file");
    }
    // Under the index's own contract, never this process's configuration: a token the build never
    // admitted is absent from the dictionary, and absence would read as a miss for every file.
    List<List<String>> tokensPerField = new ArrayList<>();
    Set<String> all = new LinkedHashSet<>();
    for (WatchRule.Field asked : fields) {
      List<String> tokens = new ArrayList<>();
      for (String value : asked.values()) {
        List<String> terms = index.contract().queryTerms(value);
        if (terms.size() != 1) {
          throw new Refused(
              "'" + value + "' is " + terms.size() + " term(s) under " + index.analyzer()
                  + "; a hunt takes exactly one per value");
        }
        String token = terms.get(0);
        if (!index.contract().isIndexable(token)) {
          throw new Refused(
              "'" + token + "' is not admitted by " + index.analyzer()
                  + ": the index never wrote it, so its silence is not absence");
        }
        tokens.add(token);
        all.add(token);
      }
      tokensPerField.add(tokens);
    }
    Map<String, TermIndex.Entry> entries;
    try {
      entries = termIndex.entriesFor(table, index, all);
    } catch (IOException | RuntimeException e) {
      // The pruner keeps every file here (a leaf it cannot read is not an absent term). A hunt
      // cannot: every-file-kept and every-file-hit are the same list.
      throw new Refused("term index unreadable for column " + column + ": " + e.getMessage(), e);
    }

    // The verdict rule is the index's, not this class's: TermIndexType.partitionByCoverage decides
    // covered-and-named = HIT, covered-and-not = ABSENT, uncovered = UNKNOWN, and it is the same
    // call the plan path's term tier makes. What differs here is only what the three are used FOR.
    IndexType.FileSet files = IndexType.FileSet.of(liveDataFiles(table, current));
    Map<String, IndexType.Partition> perToken = new java.util.LinkedHashMap<>();
    for (String token : all) {
      TermIndex.Entry entry = entries.get(token);
      perToken.put(
          token,
          TermIndexType.partitionByCoverage(
              files, index, entry == null ? new RoaringBitmap() : entry.ordinals()));
    }
    // What was never examined is a property of the coverage alone, not of any token, so it is asked
    // for as such: probing the empty posting set makes every covered file a proven absence and
    // leaves exactly the uncovered ones unknown. Reading it off one of the token partitions would
    // give the same answer and would depend on there being a token to read it from.
    RoaringBitmap unexamined =
        TermIndexType.partitionByCoverage(files, index, new RoaringBitmap()).unknown();

    List<String> hit = new ArrayList<>();
    List<String> miss = new ArrayList<>();
    List<String> unresolved = new ArrayList<>();
    boolean[] hits = new boolean[tokensPerField.size()];
    for (int ordinal : files.inPlay()) {
      String path = files.pathOf(ordinal);
      if (unexamined.contains(ordinal)) {
        unresolved.add(path); // outside coverage: not examined, so not a miss either
        continue;
      }
      for (int i = 0; i < hits.length; i++) {
        hits[i] = false;
        for (String token : tokensPerField.get(i)) {
          // A field's values are OR'ed by the rule's contract. Absent from the dictionary means
          // absent from every COVERED file — and only those, which the partition has already said.
          if (perToken.get(token).hits().contains(ordinal)) {
            hits[i] = true;
            break;
          }
        }
      }
      if (Conditions.eval(expr, hits)) {
        hit.add(path);
      } else {
        miss.add(path);
      }
    }
    // The same fail-closed reading the plan path and ScanPass use: a summary that does not PROVE
    // zero delete files is delete-bearing. The index counted raw rows, so on such a snapshot a hit
    // is a file that held the term, not a file that still returns it.
    String confidence = ScanPass.deleteBearing(current) ? "advisory" : "exact";
    // A count is printed only when it is a count. After a file leaves the table, total_count still
    // carries its occurrences and nothing can subtract them (FORMAT.md §5.6), so the field is
    // omitted rather than shown as an upper bound; the summary's counts_exact says why.
    Map<String, Long> occurrences = null;
    if (index.countsExact()) {
      occurrences = new java.util.LinkedHashMap<>();
      for (String token : all) {
        TermIndex.Entry entry = entries.get(token);
        occurrences.put(token, entry == null ? 0L : entry.totalCount());
      }
    }
    return new Result(
        table.name(), column, new ArrayList<>(all), ruleId, fields, expr, null, hit, miss,
        unresolved, current.snapshotId(), index.snapshotId(), index.analyzer(),
        index.countsExact(), index.partial(), confidence, occurrences);
  }

  /**
   * Every data file live at {@code snapshot}, in path order. The table's own list, never the
   * index's coverage list: coverage is the thing being checked.
   */
  private static List<String> liveDataFiles(Table table, Snapshot snapshot) {
    List<String> paths = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
      for (FileScanTask task : tasks) {
        paths.add(task.file().location());
      }
    } catch (IOException e) {
      throw new Refused("could not list the table's data files: " + e.getMessage(), e);
    }
    paths.sort(null);
    return paths;
  }
}
