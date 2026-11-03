//! SASL PLAIN user database for BEEP listeners (`[beep.auth]` / `--sasl-users`).

use std::collections::HashMap;

use minta_beep::sasl::Authenticator;

use crate::config::ServerAuth;

pub struct Users {
    /// user → `password` or `sha256:<hex>`.
    map: HashMap<String, String>,
}

impl Users {
    pub fn load(cfg: &ServerAuth) -> Result<Users, String> {
        let mut map = cfg.users.clone();
        if let Some(path) = &cfg.users_file {
            let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
            for (n, line) in text.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let (user, secret) = line.split_once(':').ok_or_else(|| format!("{}:{}: expected user:password", path.display(), n + 1))?;
                map.insert(user.trim().to_string(), secret.trim().to_string());
            }
        }
        if map.is_empty() {
            return Err("SASL auth is enabled but no users are configured".into());
        }
        Ok(Users { map })
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl Authenticator for Users {
    fn authenticate(&self, authzid: &str, authcid: &str, password: &str) -> Option<String> {
        // No proxy authorization: a user may only act as itself.
        if !authzid.is_empty() && authzid != authcid {
            return None;
        }
        let stored = self.map.get(authcid)?;
        let ok = match stored.strip_prefix("sha256:") {
            Some(want) => same(hex(ring::digest::digest(&ring::digest::SHA256, password.as_bytes()).as_ref()).as_bytes(), want.to_ascii_lowercase().as_bytes()),
            None => same(stored.as_bytes(), password.as_bytes()),
        };
        ok.then(|| authcid.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_and_hashed_passwords() {
        let mut users = HashMap::new();
        users.insert("alice".to_string(), "s3cret".to_string());
        // sha256("hunter2")
        users.insert("bob".to_string(), "sha256:f52fbd32b2b3b86ff88ef6c490628285f482af15ddcb29541f94bcf526a3f6c7".to_string());
        let u = Users::load(&ServerAuth { users_file: None, users }).unwrap();
        assert_eq!(u.authenticate("", "alice", "s3cret").as_deref(), Some("alice"));
        assert_eq!(u.authenticate("", "alice", "s3cre"), None);
        assert_eq!(u.authenticate("", "bob", "hunter2").as_deref(), Some("bob"));
        assert_eq!(u.authenticate("alice", "bob", "hunter2"), None);
        assert_eq!(u.authenticate("", "carol", "x"), None);
    }
}
