use serde::{Deserialize, Deserializer, Serialize};

use super::ContentDocument;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "content_type", content = "data", rename_all = "snake_case")]
pub enum ContentBody {
    StructuredContent(ContentDocument),
    HtmlContent(String),
}

impl Default for ContentBody {
    fn default() -> Self {
        ContentBody::HtmlContent(String::new())
    }
}

impl ContentBody {
    pub fn html<S: Into<String>>(s: S) -> Self {
        ContentBody::HtmlContent(s.into())
    }

    pub fn structured(doc: ContentDocument) -> Self {
        ContentBody::StructuredContent(doc)
    }

    /// Returns `true` if this body is structurally empty.
    ///
    /// - `HtmlContent`: whitespace-only HTML returns `true`.
    /// - `StructuredContent`: returns `true` only if the document has zero
    ///   blocks. A document with one whitespace-only paragraph still returns
    ///   `false`. For a "visually empty" check, walk the blocks via
    ///   `to_plain_text().trim().is_empty()` once Task 3 lands.
    pub fn is_empty(&self) -> bool {
        match self {
            ContentBody::HtmlContent(s) => s.trim().is_empty(),
            ContentBody::StructuredContent(d) => d.blocks.is_empty(),
        }
    }
}

impl From<String> for ContentBody {
    fn from(s: String) -> Self {
        ContentBody::HtmlContent(s)
    }
}

impl From<&str> for ContentBody {
    fn from(s: &str) -> Self {
        ContentBody::HtmlContent(s.to_string())
    }
}

impl<'de> Deserialize<'de> for ContentBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        // Internal tagged enum used only inside visit_map. The outer custom
        // impl lets us also accept a bare JSON string (legacy raw HTML rows).
        #[derive(Deserialize)]
        #[serde(tag = "content_type", content = "data", rename_all = "snake_case")]
        enum Tagged {
            StructuredContent(ContentDocument),
            HtmlContent(String),
        }

        struct ContentBodyVisitor;

        impl<'de> Visitor<'de> for ContentBodyVisitor {
            type Value = ContentBody;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string (legacy raw HTML) or a tagged ContentBody object")
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<ContentBody, E> {
                Ok(ContentBody::HtmlContent(s.to_owned()))
            }

            fn visit_string<E: de::Error>(self, s: String) -> Result<ContentBody, E> {
                Ok(ContentBody::HtmlContent(s))
            }

            fn visit_map<A>(self, map: A) -> Result<ContentBody, A::Error>
            where
                A: MapAccess<'de>,
            {
                let tagged = Tagged::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(match tagged {
                    Tagged::StructuredContent(d) => ContentBody::StructuredContent(d),
                    Tagged::HtmlContent(s) => ContentBody::HtmlContent(s),
                })
            }
        }

        deserializer.deserialize_any(ContentBodyVisitor)
    }
}
