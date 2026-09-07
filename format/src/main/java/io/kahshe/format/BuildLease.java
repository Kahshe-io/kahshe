package io.kahshe.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * One builder per column at a time: a lease file beside the column's index metadata. A builder
 * refuses, loudly, when another owner's unexpired lease exists, because the two tiers' metadata
 * publishes are not atomic with each other and two builders could interleave them.
 *
 * <p>This is a guard rail, not mutual exclusion — {@link FileIO} has no atomic create-if-absent on
 * object stores, so {@link #assertStillHeld} before the first publish is what narrows the exposure
 * to one round trip. A lease past {@link #TTL_MS}, or one that cannot be read, is overridden with a
 * WARN; a build that genuinely runs longer than the TTL therefore loses its lease to whoever
 * overrode it, and its own publish is refused at {@link #assertStillHeld} rather than interleaved —
 * loud rather than wrong. {@link #releaseAll} drops the leases this process holds; a build thread
 * still running past that publishes nothing, since its next {@link #assertStillHeld} finds no lease
 * and aborts.
 */
public final class BuildLease {
  private static final Logger LOG = LoggerFactory.getLogger(BuildLease.class);

  /**
   * Another live owner holds the lease. Its own type because callers want opposite things from it:
   * a CLI run or a lone indexer treats it as a refusal, a member of a fleet as another replica's
   * work, which it skips and counts.
   */
  public static final class HeldByAnother extends IllegalStateException {
    private static final long serialVersionUID = 1L;
    private final String holder;

    HeldByAnother(String message, String holder) {
      super(message);
      this.holder = holder;
    }

    /** The owner id written in the lease. */
    public String holder() {
      return holder;
    }
  }
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Comfortably above a full build of a large table, so a healthy build never races its own expiry,
   * and short enough that a lease left behind by a dead builder clears within a day.
   */
  static final long TTL_MS = 12L * 60 * 60 * 1000;

  /** The leases this process holds right now. */
  private static final Set<BuildLease> LIVE = ConcurrentHashMap.newKeySet();

  private final FileIO io;
  private final String path;
  private final String owner;
  private final AtomicBoolean released = new AtomicBoolean();

  private BuildLease(FileIO io, String path, String owner) {
    this.io = io;
    this.path = path;
    this.owner = owner;
  }

  /**
   * The lease file for one column: {@code build.lease} beside that column's term-tier metadata.
   * This is the path {@link HeldByAnother}'s message names when it tells an operator to delete a
   * lease a dead builder left behind.
   */
  public static String leasePath(String indexRoot, int fieldId) {
    return TermIndexWriter.dir(indexRoot, fieldId) + "/build.lease";
  }

  /** Something no two processes share: host, pid and a nonce. */
  public static String ownerId() {
    String host;
    try {
      host = java.net.InetAddress.getLocalHost().getHostName();
    } catch (IOException e) {
      host = "unknown-host";
    }
    return host + "/" + ProcessHandle.current().pid() + "/"
        + Long.toHexString(java.util.concurrent.ThreadLocalRandom.current().nextLong() >>> 32);
  }

  /**
   * Takes the lease or throws. {@code nowMs} is a parameter so a test can age a lease without
   * waiting twelve hours.
   */
  public static BuildLease acquire(FileIO io, String path, String owner, long nowMs) throws IOException {
    JsonNode existing = read(io, path);
    if (existing != null) {
      String holder = existing.path("owner").asText("?");
      long expiresMs = existing.path("expires-ms").asLong(0);
      if (expiresMs > nowMs && !holder.equals(owner)) {
        throw new HeldByAnother(
            "another build of this column holds " + path + " (owner " + holder + ", expires in "
                + (expiresMs - nowMs) / 1000 + " s); refusing to run two builds of one column, "
                + "since their metadata publishes would interleave. If that builder is dead, delete "
                + "the lease or wait for it to expire.",
            holder);
      }
      if (expiresMs <= nowMs) {
        LOG.warn(
            "overriding an expired build lease at {} (owner {}, expired {} s ago): treating that "
                + "build as abandoned",
            path, holder, (nowMs - expiresMs) / 1000);
      }
    }
    BuildLease lease = new BuildLease(io, path, owner);
    lease.write(nowMs);
    LIVE.add(lease);
    return lease;
  }

  /**
   * Releases every lease this process holds; the shutdown hook's call. Idempotent per lease, so
   * a build that unwinds its own {@code finally} afterwards releases nothing twice. Returns how
   * many it released.
   */
  public static int releaseAll() {
    int released = 0;
    for (BuildLease lease : LIVE) {
      if (lease.release()) {
        released++;
      }
    }
    return released;
  }

  /** Before the first publish: whoever wrote the lease last owns it, and the other aborts here. */
  public void assertStillHeld() throws IOException {
    JsonNode current = read(io, path);
    String holder = current == null ? null : current.path("owner").asText(null);
    if (!owner.equals(holder)) {
      throw new IllegalStateException(
          "build lease at " + path + " is now held by " + holder + ", not this build (" + owner
              + "); aborting before publishing so the two builds cannot interleave");
    }
  }

  /**
   * Best effort: a lease that outlives its build is overridden after {@link #TTL_MS} anyway.
   * Returns whether this call did the release (false when it was already released).
   */
  public boolean release() {
    if (!released.compareAndSet(false, true)) {
      return false;
    }
    LIVE.remove(this);
    try {
      io.deleteFile(path);
    } catch (RuntimeException e) {
      LOG.warn("could not delete build lease {}; it expires on its own", path, e);
    }
    return true;
  }

  private void write(long nowMs) throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("owner", owner);
    node.put("acquired-ms", nowMs);
    node.put("expires-ms", nowMs + TTL_MS);
    try (PositionOutputStream out = io.newOutputFile(path).createOrOverwrite()) {
      out.write(MAPPER.writeValueAsBytes(node));
    }
  }

  private static JsonNode read(FileIO io, String path) throws IOException {
    InputFile file = io.newInputFile(path);
    if (!file.exists()) {
      return null;
    }
    try (var in = file.newStream()) {
      return MAPPER.readTree(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException | RuntimeException e) {
      LOG.warn("build lease at {} is unreadable; overriding it rather than blocking every build",
          path, e);
      return null;
    }
  }
}
