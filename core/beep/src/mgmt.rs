//! Channel-0 management messages (RFC 3080 §2.3) and a minimal, safe XML reader for them.

use crate::error::{code, Error};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProfileEl {
    pub uri: String,
    /// Piggybacked initialization data (already entity/CDATA-decoded).
    pub data: Option<String>,
    /// `encoding='base64'` was present on the element.
    pub base64: bool,
}

impl ProfileEl {
    pub fn new(uri: impl Into<String>) -> Self {
        ProfileEl { uri: uri.into(), data: None, base64: false }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mgmt {
    Greeting { profiles: Vec<String>, features: Option<String>, localize: Option<String> },
    Start { number: u32, server_name: Option<String>, profiles: Vec<ProfileEl> },
    Close { number: u32, code: u32, message: Option<String> },
    Profile(ProfileEl),
    Ok,
    Error { code: u32, message: String },
}

impl Mgmt {
    pub fn parse(xml: &[u8]) -> Result<Mgmt, Error> {
        let text = std::str::from_utf8(xml).map_err(|_| bad("management message is not UTF-8"))?;
        let root = parse_xml(text).map_err(|e| bad(&e))?;
        let num_attr = |name: &str| -> Result<u32, Error> {
            let v = root.attr(name).ok_or_else(|| bad(&format!("<{}> lacks {name}", root.name)))?;
            v.trim().parse::<u32>().map_err(|_| bad(&format!("bad {name} attribute {v:?}")))
        };
        Ok(match root.name.as_str() {
            "greeting" => Mgmt::Greeting {
                profiles: root
                    .elements("profile")
                    .filter_map(|p| p.attr("uri").map(str::to_string))
                    .collect(),
                features: root.attr("features").map(str::to_string),
                localize: root.attr("localize").map(str::to_string),
            },
            "start" => {
                let number = num_attr("number")?;
                if number > 2_147_483_647 {
                    return Err(bad("channel number out of range"));
                }
                Mgmt::Start {
                    number,
                    server_name: root.attr("serverName").map(str::to_string),
                    profiles: root.elements("profile").map(profile_from).collect(),
                }
            }
            "close" => Mgmt::Close {
                number: num_attr("number")?,
                code: num_attr("code").unwrap_or(code::SUCCESS),
                message: Some(root.text()).filter(|s| !s.trim().is_empty()),
            },
            "profile" => Mgmt::Profile(profile_from(&root)),
            "ok" => Mgmt::Ok,
            "error" => Mgmt::Error { code: num_attr("code").unwrap_or(code::TRANSACTION_FAILED), message: root.text().trim().to_string() },
            other => return Err(bad(&format!("unknown management element <{other}>"))),
        })
    }

    pub fn to_xml(&self) -> String {
        match self {
            Mgmt::Greeting { profiles, features, localize } => {
                let mut attrs = String::new();
                if let Some(f) = features {
                    attrs.push_str(&format!(" features='{}'", esc(f)));
                }
                if let Some(l) = localize {
                    attrs.push_str(&format!(" localize='{}'", esc(l)));
                }
                if profiles.is_empty() {
                    format!("<greeting{attrs} />")
                } else {
                    let inner: String = profiles.iter().map(|p| format!("<profile uri='{}' />", esc(p))).collect();
                    format!("<greeting{attrs}>{inner}</greeting>")
                }
            }
            Mgmt::Start { number, server_name, profiles } => {
                let sn = server_name.as_ref().map(|s| format!(" serverName='{}'", esc(s))).unwrap_or_default();
                let inner: String = profiles.iter().map(profile_xml).collect();
                format!("<start number='{number}'{sn}>{inner}</start>")
            }
            Mgmt::Close { number, code, message } => match message {
                Some(m) => format!("<close number='{number}' code='{code}'>{}</close>", esc(m)),
                None => format!("<close number='{number}' code='{code}' />"),
            },
            Mgmt::Profile(p) => profile_xml(p),
            Mgmt::Ok => "<ok />".to_string(),
            Mgmt::Error { code, message } => format!("<error code='{code}'>{}</error>", esc(message)),
        }
    }
}

fn profile_from(e: &Element) -> ProfileEl {
    let data = e.text();
    ProfileEl {
        uri: e.attr("uri").unwrap_or_default().to_string(),
        data: if data.trim().is_empty() { None } else { Some(data) },
        base64: e.attr("encoding") == Some("base64"),
    }
}

fn profile_xml(p: &ProfileEl) -> String {
    let enc = if p.base64 { " encoding='base64'" } else { "" };
    match &p.data {
        None => format!("<profile uri='{}'{enc} />", esc(&p.uri)),
        Some(d) if !d.contains("]]>") => format!("<profile uri='{}'{enc}><![CDATA[{d}]]></profile>", esc(&p.uri)),
        Some(d) => format!("<profile uri='{}'{enc}>{}</profile>", esc(&p.uri), esc(d)),
    }
}

fn bad(msg: &str) -> Error {
    Error::Protocol(format!("channel 0: {msg}"))
}

pub fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '&' => o.push_str("&amp;"),
            '\'' => o.push_str("&apos;"),
            '"' => o.push_str("&quot;"),
            c => o.push(c),
        }
    }
    o
}

// ---------------------------------------------------------------------------
// Minimal XML reader: elements, attributes, text, CDATA, comments, PIs.
// DOCTYPE is rejected outright so entity-expansion attacks are impossible.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Element {
    pub name: String,
    pub attrs: Vec<(String, String)>,
    pub children: Vec<Node>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Element(Element),
    Text(String),
}

impl Element {
    pub fn attr(&self, name: &str) -> Option<&str> {
        self.attrs.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
    pub fn elements<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Element> + 'a {
        self.children.iter().filter_map(move |n| match n {
            Node::Element(e) if e.name == name => Some(e),
            _ => None,
        })
    }
    pub fn text(&self) -> String {
        self.children
            .iter()
            .filter_map(|n| if let Node::Text(t) = n { Some(t.as_str()) } else { None })
            .collect()
    }
}

pub fn parse_xml(s: &str) -> Result<Element, String> {
    let mut p = XmlParser { s: s.as_bytes(), i: 0, src: s };
    p.misc()?;
    let root = p.element(0)?;
    p.misc()?;
    if p.i != p.s.len() {
        return Err("trailing content after root element".into());
    }
    Ok(root)
}

struct XmlParser<'a> {
    s: &'a [u8],
    i: usize,
    src: &'a str,
}

impl<'a> XmlParser<'a> {
    fn starts(&self, pat: &str) -> bool {
        self.s[self.i..].starts_with(pat.as_bytes())
    }
    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn skip_past(&mut self, end: &str) -> Result<usize, String> {
        let start = self.i;
        match self.src[self.i..].find(end) {
            Some(p) => {
                self.i += p + end.len();
                Ok(start)
            }
            None => Err(format!("unterminated construct, expected {end:?}")),
        }
    }
    /// Whitespace, comments and processing instructions outside the root.
    fn misc(&mut self) -> Result<(), String> {
        loop {
            self.ws();
            if self.starts("<?") {
                self.skip_past("?>")?;
            } else if self.starts("<!--") {
                self.skip_past("-->")?;
            } else if self.starts("<!") {
                return Err("DOCTYPE/declarations are not allowed".into());
            } else {
                return Ok(());
            }
        }
    }
    fn name(&mut self) -> Result<String, String> {
        let start = self.i;
        while self.i < self.s.len() {
            let c = self.s[self.i];
            if c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.' | b':') || c >= 0x80 {
                self.i += 1;
            } else {
                break;
            }
        }
        if start == self.i {
            return Err("expected a name".into());
        }
        Ok(self.src[start..self.i].to_string())
    }
    fn element(&mut self, depth: usize) -> Result<Element, String> {
        if depth > 16 {
            return Err("XML nested too deeply".into());
        }
        if !self.starts("<") {
            return Err("expected '<'".into());
        }
        self.i += 1;
        let name = self.name()?;
        let mut attrs = vec![];
        loop {
            self.ws();
            if self.starts("/>") {
                self.i += 2;
                return Ok(Element { name, attrs, children: vec![] });
            }
            if self.starts(">") {
                self.i += 1;
                break;
            }
            let an = self.name()?;
            self.ws();
            if !self.starts("=") {
                return Err(format!("attribute {an} lacks '='"));
            }
            self.i += 1;
            self.ws();
            let q = *self.s.get(self.i).ok_or("unexpected end in attribute")?;
            if q != b'\'' && q != b'"' {
                return Err("attribute value must be quoted".into());
            }
            self.i += 1;
            let start = self.i;
            while self.i < self.s.len() && self.s[self.i] != q {
                self.i += 1;
            }
            if self.i >= self.s.len() {
                return Err("unterminated attribute value".into());
            }
            let raw = &self.src[start..self.i];
            self.i += 1;
            attrs.push((an, unescape(raw)?));
        }
        let mut children = vec![];
        loop {
            if self.i >= self.s.len() {
                return Err(format!("unterminated element <{name}>"));
            }
            if self.starts("</") {
                self.i += 2;
                let close = self.name()?;
                self.ws();
                if close != name || !self.starts(">") {
                    return Err(format!("mismatched closing tag </{close}> for <{name}>"));
                }
                self.i += 1;
                return Ok(Element { name, attrs, children });
            } else if self.starts("<![CDATA[") {
                self.i += 9;
                let start = self.skip_past("]]>")?;
                children.push(Node::Text(self.src[start..self.i - 3].to_string()));
            } else if self.starts("<!--") {
                self.skip_past("-->")?;
            } else if self.starts("<?") {
                self.skip_past("?>")?;
            } else if self.starts("<!") {
                return Err("declarations are not allowed".into());
            } else if self.starts("<") {
                children.push(Node::Element(self.element(depth + 1)?));
            } else {
                let start = self.i;
                while self.i < self.s.len() && self.s[self.i] != b'<' {
                    self.i += 1;
                }
                children.push(Node::Text(unescape(&self.src[start..self.i])?));
            }
        }
    }
}

fn unescape(s: &str) -> Result<String, String> {
    if !s.contains('&') {
        return Ok(s.to_string());
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(p) = rest.find('&') {
        out.push_str(&rest[..p]);
        let after = &rest[p + 1..];
        let end = after.find(';').ok_or("unterminated entity")?;
        let ent = &after[..end];
        match ent {
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "amp" => out.push('&'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ if ent.starts_with("#x") || ent.starts_with("#X") => {
                let v = u32::from_str_radix(&ent[2..], 16).map_err(|_| "bad char ref")?;
                out.push(char::from_u32(v).ok_or("bad char ref")?);
            }
            _ if ent.starts_with('#') => {
                let v: u32 = ent[1..].parse().map_err(|_| "bad char ref")?;
                out.push(char::from_u32(v).ok_or("bad char ref")?);
            }
            _ => return Err(format!("unknown entity &{ent};")),
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_examples() {
        let g = Mgmt::parse(b"<greeting>\r\n   <profile uri='http://iana.org/beep/SASL/OTP' />\r\n</greeting>\r\n").unwrap();
        assert_eq!(
            g,
            Mgmt::Greeting { profiles: vec!["http://iana.org/beep/SASL/OTP".into()], features: None, localize: None }
        );
        let s = Mgmt::parse(
            b"<start number='1' serverName=\"x\">\n<profile uri='http://iana.org/beep/TLS'>\n<![CDATA[<ready />]]>\n</profile>\n</start>",
        )
        .unwrap();
        match s {
            Mgmt::Start { number: 1, server_name: Some(sn), profiles } => {
                assert_eq!(sn, "x");
                assert_eq!(profiles[0].uri, "http://iana.org/beep/TLS");
                assert_eq!(profiles[0].data.as_deref().map(str::trim), Some("<ready />"));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(Mgmt::parse(b"<ok />").unwrap(), Mgmt::Ok);
        assert_eq!(
            Mgmt::parse(b"<error code='550'>still working &amp; busy</error>").unwrap(),
            Mgmt::Error { code: 550, message: "still working & busy".into() }
        );
        assert_eq!(
            Mgmt::parse(b"<?xml version='1.0'?><close number='0' code='200' />").unwrap(),
            Mgmt::Close { number: 0, code: 200, message: None }
        );
    }

    #[test]
    fn roundtrip() {
        let msgs = vec![
            Mgmt::Greeting { profiles: vec!["a'b".into(), "urn:x".into()], features: None, localize: None },
            Mgmt::Greeting { profiles: vec![], features: None, localize: None },
            Mgmt::Start { number: 3, server_name: None, profiles: vec![ProfileEl { uri: "u".into(), data: Some("<ready />".into()), base64: false }] },
            Mgmt::Close { number: 1, code: 200, message: Some("bye <now>".into()) },
            Mgmt::Profile(ProfileEl::new("urn:minta:beep:rpc:1")),
            Mgmt::Ok,
            Mgmt::Error { code: 553, message: "bad & worse".into() },
        ];
        for m in msgs {
            assert_eq!(Mgmt::parse(m.to_xml().as_bytes()).unwrap(), m, "{}", m.to_xml());
        }
    }

    #[test]
    fn rejects_hostile_input() {
        assert!(Mgmt::parse(b"<!DOCTYPE x [<!ENTITY a 'b'>]><ok/>").is_err());
        assert!(Mgmt::parse(b"<ok>&a;</ok>").is_err());
        assert!(Mgmt::parse(b"<start number='x'/>").is_err());
        assert!(Mgmt::parse(b"<a><b></a></b>").is_err());
        assert!(Mgmt::parse("<ok>".repeat(40).as_bytes()).is_err());
    }
}
