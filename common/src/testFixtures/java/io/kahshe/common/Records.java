package io.kahshe.common;

import java.lang.reflect.RecordComponent;
import java.util.Map;

/**
 * Test-only: a record with one or more components replaced, looked up by name at any depth of the
 * record tree, so a test overrides a setting by the leaf it changes whichever per-module record
 * holds it. A name no record has is an error, not a silent no-op.
 */
public final class Records {
  private Records() {}

  @SuppressWarnings("unchecked")
  public static <T extends java.lang.Record> T with(T base, Map<String, Object> overrides) {
    Object result = base;
    for (Map.Entry<String, Object> override : overrides.entrySet()) {
      Object next = withLeaf(result, override.getKey(), override.getValue());
      if (next == null) {
        throw new IllegalArgumentException("no such Config component: " + override.getKey());
      }
      result = next;
    }
    return (T) result;
  }

  /** {@code record} rebuilt with {@code key} replaced at any depth; null when no such component. */
  private static Object withLeaf(Object record, String key, Object value) {
    try {
      RecordComponent[] components = record.getClass().getRecordComponents();
      Class<?>[] types = new Class<?>[components.length];
      Object[] values = new Object[components.length];
      boolean found = false;
      for (int i = 0; i < components.length; i++) {
        types[i] = components[i].getType();
        Object current = components[i].getAccessor().invoke(record);
        if (components[i].getName().equals(key)) {
          values[i] = value;
          found = true;
        } else if (current != null && current.getClass().isRecord()) {
          Object nested = withLeaf(current, key, value);
          values[i] = nested != null ? nested : current;
          found |= nested != null;
        } else {
          values[i] = current;
        }
      }
      return found ? record.getClass().getDeclaredConstructor(types).newInstance(values) : null;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("could not rebuild a test record", e);
    }
  }
}
