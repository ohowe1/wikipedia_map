pub mod pool;

use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{BufRead, BufReader, Read};
use log::debug;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ContentPage {
    pub title: String,
    pub title_hash: u64,
    pub links: Vec<u64>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
    pub const TITLE: &'static [u8] = b"title";
    pub const TEXT: &'static [u8] = b"text";
    pub const REDIRECT: &'static [u8] = b"redirect";
}

enum PageState {
    InTitle,
    InText,
    Out
}

pub struct ArticleParser {
    content_pages: Vec<ContentPage>,
    redirects: HashMap<u64, u64>,

    read_buffer: Vec<u8>,
}


impl ArticleParser {
    const UPDATE_FREQ: u64 = 1024 * 1024 * 500;
    const BUFFER_SIZE: usize = 1024 * 1024;

    pub fn new() -> Self {
        Self {
            content_pages: Vec::new(),
            redirects: HashMap::new(),
            read_buffer: Vec::with_capacity(Self::BUFFER_SIZE),
        }
    }

    pub fn content_pages(&self) -> &Vec<ContentPage> {
        &self.content_pages
    }

    pub fn redirects(&self) -> &HashMap<u64, u64> {
        &self.redirects
    }

    fn get_title_hash(title: &str) -> u64 {
        let mut hasher = DefaultHasher::default();
        // don't do stuff with first character if empty
        if title.is_empty() {
            title.hash(&mut hasher);
            return hasher.finish();
        }
        // odds of has collision are very low, so hopefully won't happen
        // todo: maybe make it so it deals with has collisions well

        // hash with capital first character like is wikipedia standard
        // this is wacky
        let first_char = title.char_indices().next().expect("Empty title");
        title[..first_char.0 + first_char.1.len_utf8()].to_uppercase().hash(&mut hasher);
        title[first_char.0 + first_char.1.len_utf8()..].hash(&mut hasher);

        hasher.finish()
    }

    pub fn merge_with(&mut self, other: ArticleParser) {
        self.content_pages.extend(other.content_pages);
        self.redirects.extend(other.redirects);
    }
    pub fn parse_xml<R: Read, F>(&mut self, content: &mut BufReader<R>, read_until: Option<u64>, progress_callback: Option<F>)
    where F: Fn(u64, Option<u64>) {
        let mut reader = Reader::from_reader(content);
        reader.config_mut().allow_unmatched_ends = true;

        // no need to update ever if there is no progress_callback
        let mut last_update: Option<u64> = if progress_callback.is_some() { Some(0) } else { None };
        loop {
            if let Some(read_until) = read_until {
                if reader.buffer_position() > read_until {
                    break;
                }
            }
            self.read_buffer.clear();
            match reader.read_event_into(&mut self.read_buffer).unwrap() {
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

                    let buffer_position = reader.buffer_position();
                    if last_update.is_some() && buffer_position > last_update.unwrap() + Self::UPDATE_FREQ {
                        if let Some(ref progress_callback) = progress_callback {
                            progress_callback(buffer_position, read_until);
                            last_update = Some(buffer_position);
                        }
                    }
                }
                Event::Eof => break,
                _ => {}
            }
        }
    }

    pub fn repair_redirects(&mut self) {
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

    fn get_links(content: &str) -> Vec<u64> {
        // Links are in the form of [[Link#Section|Alias]]
        // We want to ignore file, interlanguage links, and category links (are like [[File:Link]], [[es:Link]]) and only care about the link part

        let mut links = Vec::new();
        let mut start = 0;

        while let Some(start_idx) = content[start..].find("[[") {
            let start_pos = start + start_idx + 2;
            if let Some(end_idx) = content[start_pos..].find("]]") {
                let end_pos = start_pos + end_idx;
                start = end_pos + 2;

                let link = &content[start_pos..end_pos];
                // todo: maybe tag with categories, and make sure its not line english namespace
                // ignore links with a namespace
                if link.contains(":") {
                    continue;
                }
                let relevant_end_pos = link.find('#').or_else(|| link.find('|')).unwrap_or(link.len());
                links.push(Self::get_title_hash(&link[..relevant_end_pos]));
            } else {
                break;
            }
        }

        links
    }

    fn parse_page<R: BufRead>(&mut self, reader: &mut Reader<R>) -> Page {
        let mut state = PageState::Out;

        let mut title = String::new();
        let mut links = Vec::new();
        let mut redirect: Option<String> = None;

        loop {
            match reader.read_event_into(&mut self.read_buffer).unwrap() {
                Event::Empty(ref e) => {
                    let name = e.name();
                    match name.as_ref() {
                        PageTags::REDIRECT => {
                            // Search for title attribute
                            if let Some(Ok(attr)) = e.attributes().find(|x| x.as_ref().map_or(false, |a| a.key.as_ref() == PageTags::TITLE)) {
                                redirect = Some(String::from_utf8_lossy(attr.value.as_ref()).to_string());
                            }
                        }
                        _ => {}
                    }
                }
                Event::Start(ref e) => {
                    let name = e.name();
                    match name.as_ref() {
                        PageTags::TITLE => {
                            state = PageState::InTitle;
                        }
                        PageTags::TEXT => {
                            state = PageState::InText;
                        }
                        _ => {
                            match state {
                                PageState::Out => {}
                                _ => {
                                }
                            }
                        }
                    }
                }
                Event::Text(ref e) => {
                    let content = e.unescape().unwrap();
                    match state {
                        PageState::InTitle => {
                            title = content.to_string().replace('_', " ");
                        }
                        PageState::InText => {
                            // Get the content of links in the form of [[]] excluding ones that reference files and anything after | or #
                            // TODO: deal with lowercase/uppercase first letter
                            links = Self::get_links(&content);
                        }
                        PageState::Out => {}
                    }
                }
                Event::End(ref e) => {
                    match e.name().as_ref() {
                        PageTags::PAGE => {
                            break;
                        }
                        _ => {
                            state = PageState::Out;
                        }
                    }
                }
                Event::Eof => break,
                _ => {}
            }
        }

        if redirect.is_some() {
            Page::Redirect(Redirect {
                title_hash: Self::get_title_hash(&title),
                to_hash: Self::get_title_hash(&redirect.unwrap()),
            })
        } else {
            Page::Content(ContentPage {
                title_hash: Self::get_title_hash(&title),
                title,
                links,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn hash_matches() {
        init();

        // Should reference same article
        let link1 = "WikipediaLink";
        let link1_alias = "wikipediaLink";

        // the L case is different and not the first character, so should be different
        let link2 = "wikipedialink";
        let link3 = "NotTheSame";

        let link1_hash = ArticleParser::get_title_hash(link1);
        assert_eq!(link1_hash, ArticleParser::get_title_hash(link1));
        assert_eq!(link1_hash, ArticleParser::get_title_hash(link1_alias));

        assert_ne!(link1_hash, ArticleParser::get_title_hash(link2));
        assert_ne!(link1_hash, ArticleParser::get_title_hash(link3));
    }
    
    fn make_content_page(title: &str, links: Vec<u64>) -> ContentPage {
        ContentPage {
            links,
            title: title.to_string(),
            title_hash: ArticleParser::get_title_hash(title),
        }
    }

    #[test]
    fn merge_parsers() {
        init();

        let mut parser1 = ArticleParser::new();
        parser1.redirects.extend([(1, 2), (3, 4)]);
        parser1.content_pages.extend([
            make_content_page("Article 1", vec![123]),
            make_content_page("Article 2", vec![456]),
        ]);

        let mut parser2 = ArticleParser::new();
        parser2.redirects.extend([(5, 6), (7, 8)]);
        parser2.content_pages.extend([
            make_content_page("Article 3", vec![789]),
            make_content_page("Article 4", vec![101]),
        ]);

        parser1.merge_with(parser2);

        assert_eq!(
            parser1.redirects,
            HashMap::from([(1, 2), (3, 4), (5, 6), (7, 8)])
        );
        assert_eq!(
            parser1.content_pages,
            vec![
                make_content_page("Article 1", vec![123]),
                make_content_page("Article 2", vec![456]),
                make_content_page("Article 3", vec![789]),
                make_content_page("Article 4", vec![101]),
            ]
        );
    }

    #[test]
    fn get_links() {
        init();
        let example_article = "This is a [[Wikipedia]] article. It can contain many [[Web Link#Web|links]] that have [[Alias|aliases]] and also reference files, categories and interlanguage links: [[File:A file]], [[Category::A Category]], [[fr:french]]. These should not be included. Hopefully it doesn't die if there's a non-ended link: [[";

        assert_eq!(
            ArticleParser::get_links(example_article),
            Vec::from(["Wikipedia", "Web Link", "Alias"].map(|x| ArticleParser::get_title_hash(x)))
        );
    }
}
