use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct Counters {
    pub requests: AtomicU64,
    pub errors: AtomicU64,
    pub in_flight: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> [u64; 5] {
        [
            self.requests.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.in_flight.load(Ordering::Relaxed),
            self.bytes_in.load(Ordering::Relaxed),
            self.bytes_out.load(Ordering::Relaxed),
        ]
    }
}

/// Process-wide counters keyed by `(kind, name)`, e.g. `("http", "127.0.0.1:8080")`.
#[derive(Default)]
pub struct Metrics {
    map: Mutex<BTreeMap<(String, String), &'static Counters>>,
}

pub struct InFlight<'a>(&'a Counters);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Metrics {
    pub fn get(&self, kind: &str, name: &str) -> &'static Counters {
        let mut m = self.map.lock().unwrap();
        m.entry((kind.to_string(), name.to_string())).or_insert_with(|| Box::leak(Box::default()))
    }

    pub fn start(c: &'static Counters) -> InFlight<'static> {
        c.requests.fetch_add(1, Ordering::Relaxed);
        c.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlight(c)
    }

    pub fn json(&self) -> serde_json::Value {
        let m = self.map.lock().unwrap();
        let mut out = serde_json::Map::new();
        for ((kind, name), c) in m.iter() {
            let [req, err, inf, bin, bout] = c.snapshot();
            out.insert(
                format!("{kind}:{name}"),
                serde_json::json!({"requests": req, "errors": err, "in_flight": inf, "bytes_in": bin, "bytes_out": bout}),
            );
        }
        serde_json::Value::Object(out)
    }

    pub fn prometheus(&self) -> String {
        let m = self.map.lock().unwrap();
        let mut s = String::new();
        let series = [
            ("beepgw_requests_total", "counter", 0),
            ("beepgw_errors_total", "counter", 1),
            ("beepgw_in_flight", "gauge", 2),
            ("beepgw_bytes_in_total", "counter", 3),
            ("beepgw_bytes_out_total", "counter", 4),
        ];
        for (metric, kind, idx) in series {
            s.push_str(&format!("# TYPE {metric} {kind}\n"));
            for ((k, name), c) in m.iter() {
                let v = c.snapshot()[idx];
                s.push_str(&format!("{metric}{{kind=\"{k}\",name=\"{}\"}} {v}\n", name.replace('"', "")));
            }
        }
        s
    }
}
