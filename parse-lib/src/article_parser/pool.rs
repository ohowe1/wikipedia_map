use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::thread;
use log::debug;
use crate::article_parser::ArticleParser;

pub struct Pool {
    pub pool_size: u32,
}

impl Pool {
    pub fn new(pool_size: u32) -> Self {
        let pool = Self {
            pool_size
        };
        
        pool
    }
    
    pub fn process_file(&mut self, file_path: &str) {
        // TODO
        let file_len: u64 = 1024 * 1024 * 1024 * 10;

        let mut parsers = Vec::new();
        for _i in 0..self.pool_size {
            parsers.push(ArticleParser::new());
        }

        let per_parser = file_len / parsers.len() as u64;
        let mut thread_handles = Vec::new();
        for i in (0..parsers.len()).rev() {
            // TODO: fix all this, ownership of data
            let mut parser = parsers.pop().unwrap();
            let path_copy = file_path.to_owned();
            thread_handles.push(
                thread::spawn(move || {
                    let mut reader = BufReader::new(File::open(path_copy).unwrap());
                    reader.seek(SeekFrom::Start(per_parser * i as u64)).expect("Failed to seek to parser start");

                    parser.parse_xml(&mut reader, Some(per_parser), None);

                    parser
            }));
        }

        for _i in (0..thread_handles.len()).rev() {
            let handle = thread_handles.pop().unwrap();
            let parser = handle.join().unwrap();

            parsers.insert(0, parser);
        }

        for _i in (1..parsers.len()).rev() {
            let parser = parsers.pop().unwrap();

            parsers[0].merge_with(parser);
        }
        debug!("{} {}", parsers[0].content_pages.len(), parsers[0].redirects.len());
        // parsers[0].repair_redirects();
    }
}
