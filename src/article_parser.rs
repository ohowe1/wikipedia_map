mod pool;

use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::BufReader;
use std::thread::current;
use log::debug;
use regex::Regex;
use serde::{Serialize, Serializer};
use serde::ser::SerializeStruct;

struct ContentPage {
    title: String,
    title_hash: u64,
    links: Vec<u64>,
}

impl Serialize for ContentPage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer,
    {
        let mut s = serializer.serialize_struct("ContentPage", 3)?;
        s.serialize_field("title", &self.title)?;
        s.serialize_field("title_hash", &self.title_hash)?;
        s.serialize_field("links", &self.links)?;
        s.end()
    }
}

struct Redirect {
    title_hash: u64,
    to_hash: u64,
}

enum Page {
    Content(ContentPage),
    Redirect(Redirect),
}
struct PageTags;
impl PageTags {
    pub const PAGE: &'static [u8] = b"page";
    pub const PARENT: &'static [u8] = b"parent";
    pub const TITLE: &'static [u8] = b"title";
    pub const TEXT: &'static [u8] = b"text";
    pub const REDIRECT: &'static [u8] = b"redirect";
}

struct ArticleParser {
    content_pages: Vec<ContentPage>,
    redirects: HashMap<u64, u64>,
    
    link_regex: Regex,
}
impl ArticleParser {
    const UPDATE_FREQ: usize = 1024 * 1024;

    pub fn new() -> Self {
        Self {
            content_pages: Vec::new(),
            redirects: HashMap::new(),
            
            link_regex: Regex::new(r"\[\[(?!File:)([^|#\]]+)").unwrap(),
        }
    }

    fn get_hash(&mut self, title: &str) -> u64 {
        let mut hasher = DefaultHasher::default();
        // odds of has collision are very low, so hopefully won't happen
        // todo: maybe make it so it deals with has collisions well
        title.hash(&mut hasher);
        let h = hasher.finish();

        h
    }

    pub fn merge_with(&mut self, other: ArticleParser) {
        self.content_pages.extend(other.content_pages);
        self.redirects.extend(other.redirects);
        self.repair_redirects();
    }
    pub fn parse_xml<R>(&mut self, content: &BufReader<R>, read_until: Option<usize>, progress_callback: Option<fn(usize, Option(usize))>) {
        // TODO: deal with start pos
        let mut reader = Reader::from_reader(content);

        // no need to update ever if there is no progress_callback
        let mut last_update = if progress_callback.is_some() { 0 } else { content.len() };
        loop {
            if let Some(read_until) = read_until {
                if reader.buffer_position() > read_until {
                    break;
                }
            }
            match reader.read_event().unwrap() {
                Event::Start(ref e) => {
                    if e.name().as_ref() == PageTags::PAGE {
                        let article = self.parse_page(&mut reader);

                        match article {
                            Page::Content(content) => self.content_pages.push(content),
                            Page::Redirect(redirect) => {
                                self.redirects.insert(redirect.title_hash, redirect.to_hash);
                            }
                        }
                    }

                    let buffer_position = reader.buffer_position() as usize;
                    if buffer_position > last_update + Self::UPDATE_FREQ {
                        if let Some(progress_callback) = progress_callback {
                            progress_callback(buffer_position, read_until);
                            last_update = buffer_position;
                        }
                    }
                }
                Event::Eof => break,
                _ => {}
            }
        }
    }
    
    fn repair_redirects(&mut self) {
        let mut fix_count = 0;

        for link in &mut self.content_pages {
            for link_hash in &mut link.links {
                let mut current_hash = *link_hash;

                while let Some(&redirect_target) = self.redirects.get(&current_hash) {
                    current_hash = redirect_target;
                }

                let final_hash = current_hash;

                current_hash = *link_hash;
                while let Some(&redirect_target) = self.redirects.get(&current_hash) {
                    // Update all entries in the chain to point to the final redirect target
                    self.redirects.insert(current_hash, final_hash);
                    current_hash = redirect_target;
                }

                if current_hash != *link_hash {
                    fix_count += 1;
                    *link_hash = current_hash;
                }
            }
        }

        debug!("Fixed {fix_count} links that pointed to redirects.");
    }

    
    fn parse_page<T>(&mut self, reader: &mut Reader<T>) -> Page {
        let mut title = String::new();
        let mut links = Vec::new();
        let mut redirect: Option<String> = None;

        loop {
            match reader.read_event().unwrap() {
                Event::Start(ref e) => {
                    let name = e.name().as_ref();

                    match name {
                        PageTags::TITLE => {
                            title = reader.read_text(name).unwrap_or_default().replace('_', ' ');
                        }
                        PageTags::TEXT => {
                            let text = reader.read_text(name).unwrap_or_default();

                            // Get the content of links in the form of [[]] excluding ones that reference files and anything after | or #
                            // TODO: deal with lowercase/uppercase first letter
                            for cap in self.link_regex.captures_iter(&text) {
                                links.push(self.get_hash(&cap[1]));
                            }
                        }
                        PageTags::REDIRECT => {
                            redirect = Some(reader.read_text(name).unwrap_or_default());
                        }
                        _ => {}
                    }
                }
                Event::End(ref e) => {
                    if e.name().as_ref() == b"page" {
                        break
                    }
                }
                Event::Eof => break,
                _ => {}
            }
        }

        if redirect.is_some() {
            Page::Redirect(Redirect {
                title_hash: self.get_hash(&title),
                to_hash: self.get_hash(redirect.unwrap()),
            })
        } else {
            Page::Content(ContentPage {
                title,
                title_hash: self.get_hash(&title),
                links,
            })
        }
    }
}