use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::thread;
use log::debug;
use crate::article_parser::{ArticleParser, ContentPage};

pub fn process_file(file_path: &str, pool_size: u32) -> (Vec<ContentPage>, HashMap<u64, u64>) {
    // TODO
    let file_len: u64 = 1024 * 1024 * 1024 * 10;
    // let file_len: u64 = 105373313207;

    let mut parsers = Vec::new();
    for _i in 0..pool_size {
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

                parser.parse_xml(&mut reader, Some(per_parser), Some(move |progress: u64, total: Option<u64>| {
                    debug!("Parser {} of {} / {}", i, progress, total.unwrap_or(file_len - per_parser * i as u64));
                }));
                parser
            }));
    }

    for _i in (0..thread_handles.len()).rev() {
        let handle = thread_handles.pop().unwrap();
        let parser = handle.join().unwrap();

        parsers.insert(0, parser);
    }

    let mut full_parser = parsers.pop().unwrap();
    for _i in (0..parsers.len()).rev() {
        let parser = parsers.pop().unwrap();

        full_parser.merge_with(parser);
    }
    // full_parser.repair_redirects();

    (full_parser.content_pages, full_parser.redirects)
}
