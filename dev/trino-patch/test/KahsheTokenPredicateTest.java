package io.trino.plugin.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.expressions.Expression;
import org.junit.jupiter.api.Test;

/**
 * The first tests the Trino overlay has ever had.
 *
 * <p>kahshe's posture document records `dev/trino-patch` as having no harness, and records the cost of
 * that in the same breath: the fix for its worst defect — a recogniser that accepted a BARE column
 * as well as {@code lower(col)} — carries the note "No test". That defect was a false negative, the
 * one error class the system may never produce. The analyzer lower-cases BEFORE it tokenises, so a
 * row holding {@code TRACE00d5c571b1547b51} satisfies the boundary regex on the raw column while
 * its only token is {@code trace00d5c571b1547b51}. Recognising the bare form turns that into a
 * catalog MATCH, the file is pruned, and the row is gone with no error and no metric.
 *
 * <p>These run against the real {@code io.trino:trino-iceberg:483} artifacts the build already
 * resolves for {@code compileTrinoPatch}. No Docker, no cluster, nothing installed.
 */
class KahsheTokenPredicateTest {
    private static final String COLUMN = "msg";
    private static final String TOKEN = "00d5c571b1547b51";
    private static final String BOUNDARY_REGEX = "(^|[^a-z0-9])" + TOKEN + "([^a-z0-9]|$)";
    // The compound form, verbatim what kahshe's Analyzer.matchPattern("10.0.4.17") produces; the
    // same literal is asserted on that side (AnalyzerV3Test), which is how the two are kept equal.
    private static final String COMPOUND_REGEX =
            "(^|[^a-z0-9.:_-])[.:_-]*10\\.0\\.4\\.17[.:_-]*([^a-z0-9.:_-]|$)";

    private static Variable column()
    {
        return new Variable(COLUMN, VarcharType.VARCHAR);
    }

    private static Constant pattern(String regex)
    {
        return new Constant(Slices.utf8Slice(regex), VarcharType.VARCHAR);
    }

    private static Call call(String name, ConnectorExpression... arguments)
    {
        return new Call(BooleanType.BOOLEAN, new FunctionName(name), List.of(arguments));
    }

    /** The one form that is sound: regexp_like(lower(col), '<boundary>'). */
    private static Call soundForm()
    {
        return call("regexp_like", call("lower", column()), pattern(BOUNDARY_REGEX));
    }

    private static Map<String, ColumnHandle> assignments()
    {
        ColumnIdentity identity =
                new ColumnIdentity(3, COLUMN, ColumnIdentity.TypeCategory.PRIMITIVE, List.of());
        IcebergColumnHandle handle = new IcebergColumnHandle(
                identity, VarcharType.VARCHAR, List.of(), VarcharType.VARCHAR, true, Optional.empty());
        return Map.of(COLUMN, handle);
    }

    private static List<Expression> recognise(ConnectorExpression expression)
    {
        return IcebergSplitSource.kahsheTokenPredicates(
                new Constraint(TupleDomain.all(), expression, assignments()));
    }

    /** The supported form is recognised, and becomes the sentinel kahshe strips back out. */
    @Test
    void theLowerFormBecomesASentinelPredicate()
    {
        List<Expression> found = recognise(soundForm());
        assertEquals(1, found.size(), "the documented form was not recognised: " + found);
        String rendered = found.get(0).toString();
        assertTrue(rendered.contains("__kahshe_match__" + COLUMN), rendered);
        assertTrue(rendered.contains(TOKEN), rendered);
    }

    /**
     * The soundness property, and the reason this file exists.
     *
     * <p>A bare column is NOT equivalent to {@code lower(col)} and must not be recognised. It costs
     * a full scan, which is the safe direction; recognising it loses rows.
     *
     * <p>Verified by breaking it: accepting the bare column in {@code kahsheTokenPredicate} — the
     * behaviour this overlay actually shipped with — makes this find one predicate.
     */
    @Test
    void aBareColumnIsNotRecognisedNoMatterHowWellTheRegexMatches()
    {
        List<Expression> found = recognise(call("regexp_like", column(), pattern(BOUNDARY_REGEX)));
        assertTrue(
                found.isEmpty(),
                "a bare column was turned into a token MATCH; the analyzer lower-cases before it "
                        + "tokenises, so this prunes files whose rows genuinely match: " + found);
    }

    /**
     * Under OR or NOT, nothing is collected.
     *
     * <p>A token predicate becomes a CATALOG-side scan filter, and a scan filter applies to the
     * whole scan. Pushing one branch of a disjunction would prune the files the other branch still
     * needs. Only conjunctions are walked.
     */
    @Test
    void aTokenPredicateUnderOrIsNotPushedDown()
    {
        assertTrue(recognise(call("$or", soundForm(), soundForm())).isEmpty());
        assertTrue(recognise(call("$not", soundForm())).isEmpty());
        assertEquals(
                2,
                recognise(call("$and", soundForm(), soundForm())).size(),
                "conjunctions must still be walked, or this test would pass with the feature gone");
    }

    /** Anything that is not the exact documented shape costs a scan rather than being guessed at. */
    @Test
    void otherShapesAreLeftAlone()
    {
        // a regex that is not the token boundary form -- a substring, which tokens cannot answer
        assertTrue(recognise(call("regexp_like", call("lower", column()), pattern(".*abc.*"))).isEmpty());
        // a different function entirely
        assertTrue(recognise(call("regexp_replace", call("lower", column()), pattern(BOUNDARY_REGEX))).isEmpty());
        // lower() over something that is not a column
        assertTrue(recognise(call("regexp_like", call("lower", pattern("x")), pattern(BOUNDARY_REGEX))).isEmpty());
        // A NESTED field. isBaseColumn() is false for a projected column, and the index cannot
        // cover one -- IndexBuilder refuses nested columns outright -- so recognising this would
        // emit a sentinel naming a column the term dictionary has never seen. Absence prunes, so
        // that is a false negative rather than a miss.
        ColumnIdentity nested =
                new ColumnIdentity(7, "payload", ColumnIdentity.TypeCategory.PRIMITIVE, List.of());
        IcebergColumnHandle projected = new IcebergColumnHandle(
                nested, VarcharType.VARCHAR, List.of(1), VarcharType.VARCHAR, true, Optional.empty());
        assertTrue(
                IcebergSplitSource.kahsheTokenPredicates(
                                new Constraint(TupleDomain.all(), soundForm(), Map.of(COLUMN, projected)))
                        .isEmpty(),
                "a projected/nested column was recognised as a token predicate");
    }

    /**
     * Recorded rather than tested, because Trino makes it unreachable: {@code Constraint}'s own
     * constructor refuses assignments that do not cover every variable in the expression
     * ("assignments must cover all variables in expression"), so the recogniser's null check on
     * {@code assignments.get(...)} cannot fire through the engine. Keep the check — it costs
     * nothing and this file documents WHY it never fires, which is the thing a later reader would
     * otherwise delete as dead code.
     */
    @Test
    void trinoItselfGuaranteesEveryVariableIsAssigned()
    {
        assertEquals(1, recognise(soundForm()).size());
    }

    /**
     * A compound needle (an IP, a UUID, dashed hex) is recognised only in the compound form, whose
     * boundary is the analyzer's run-and-strip rule. The plain-token boundary with a dotted needle
     * is NOT recognised: {@code 10.0.4.17.1} satisfies it while the index holds no such compound,
     * so turning it into a match would prune the file that row is in. Verified red by admitting
     * separators into the token regex.
     */
    @Test
    void aCompoundNeedleIsRecognisedOnlyInTheCompoundForm()
    {
        List<Expression> found = recognise(call("regexp_like", call("lower", column()), pattern(COMPOUND_REGEX)));
        assertEquals(1, found.size(), "the compound form was not recognised: " + found);
        String rendered = found.get(0).toString();
        assertTrue(rendered.contains("10.0.4.17"), "the needle is unescaped: " + rendered);

        String dottedInTokenForm = "(^|[^a-z0-9])10\\.0\\.4\\.17([^a-z0-9]|$)";
        assertTrue(
                recognise(call("regexp_like", call("lower", column()), pattern(dottedInTokenForm))).isEmpty(),
                "a dotted needle under the token boundary is unsound and must not be recognised");
        assertTrue(
                recognise(call("regexp_like", call("lower", column()), pattern(
                        "(^|[^a-z0-9.:_-])[.:_-]*\\.abc[.:_-]*([^a-z0-9.:_-]|$)"))).isEmpty(),
                "a needle that starts with a separator is not an analyzer term");
    }
}
