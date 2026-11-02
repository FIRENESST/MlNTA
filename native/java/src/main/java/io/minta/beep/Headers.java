package io.minta.beep;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Ordered, case-insensitive multimap of header fields. */
public final class Headers implements Iterable<Map.Entry<String, String>> {
    private final List<String[]> items = new ArrayList<>();

    public Headers() {}

    public static Headers of(String... kv) {
        Headers h = new Headers();
        for (int i = 0; i + 1 < kv.length; i += 2) h.add(kv[i], kv[i + 1]);
        return h;
    }

    public static Headers from(Map<String, String> m) {
        Headers h = new Headers();
        if (m != null) m.forEach(h::add);
        return h;
    }

    public Headers add(String name, String value) {
        items.add(new String[] {name, value});
        return this;
    }

    /** Replace all values of {@code name}. */
    public Headers set(String name, String value) {
        remove(name);
        return add(name, value);
    }

    public Headers remove(String name) {
        items.removeIf(e -> e[0].equalsIgnoreCase(name));
        return this;
    }

    public String get(String name) {
        for (String[] e : items) if (e[0].equalsIgnoreCase(name)) return e[1];
        return null;
    }

    public List<String> getAll(String name) {
        List<String> out = new ArrayList<>();
        for (String[] e : items) if (e[0].equalsIgnoreCase(name)) out.add(e[1]);
        return out;
    }

    public int size() {
        return items.size();
    }

    public Headers copy() {
        Headers h = new Headers();
        for (String[] e : items) h.add(e[0], e[1]);
        return h;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        Iterator<String[]> it = items.iterator();
        return new Iterator<Map.Entry<String, String>>() {
            public boolean hasNext() { return it.hasNext(); }
            public Map.Entry<String, String> next() {
                String[] e = it.next();
                return new AbstractMap.SimpleImmutableEntry<>(e[0], e[1]);
            }
        };
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String[] e : items) sb.append(e[0]).append(": ").append(e[1]).append("\r\n");
        return sb.toString();
    }
}
