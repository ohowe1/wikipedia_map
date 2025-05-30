use std::option::Option;
use std::collections::HashMap;
use std::fs::{File};
use std::io::{BufReader, Seek, SeekFrom};
use std::{fs, thread};
use log::debug;
use crate::article_parser::{ArticleParser, ContentPage};

fn get_file_len(file_path: &str) -> Option<u64> {
    match fs::metadata(file_path) {
        Ok(metadata) => {
            Option::from(metadata.len())
        }
        Err(_) => {
            None
        }
    }
}

pub fn process_file(file_path: &str, pool_size: u64) -> (Vec<ContentPage>, HashMap<u64, u64>) {
    let file_len = get_file_len(file_path).expect("Could not get file length");

    // Create vector of parsers
    let mut parsers = Vec::new();
    for _i in 0..pool_size {
        parsers.push(ArticleParser::new());
    }

    // how many bytes each parser has to handle
    let per_parser = file_len / pool_size;

    // start the threads, and give them ownership of the parsers. Go through in reverse so pop is accurate to i
    let mut thread_handles = Vec::new();
    let parser_len = parsers.len();
    for i in (0..parsers.len()).rev() {
        // TODO: fix all this, ownership of data
        let mut parser = parsers.pop().unwrap();
        let path_copy = file_path.to_owned();
        thread_handles.push(
            thread::spawn(move || {
                let mut reader = BufReader::new(File::open(path_copy).unwrap());
                // Seek parser to where this one starts
                reader.seek(SeekFrom::Start(per_parser * i as u64)).expect("Failed to seek to parser start");

                // go till end of file on the first parser
                parser.parse_xml(&mut reader, if i != parser_len - 1 { Some(per_parser) } else { None }, Some(move |progress: u64, total: Option<u64>| {
                    debug!("Parser {} of {} / {}", i, progress, total.unwrap_or(file_len - per_parser * i as u64));
                }));
                parser
            }));
    }

    // wait for threads to finish and regain ownership of the parsers to parsers vec
    // go through in reverse to restore the previous order
    for _i in (0..thread_handles.len()).rev() {
        let handle = thread_handles.pop().unwrap();
        let parser = handle.join().unwrap();

        parsers.push(parser);
    }

    // combine each parser into one "full" one
    let mut full_parser = parsers.pop().unwrap();
    for _i in (0..parsers.len()).rev() {
        let parser = parsers.pop().unwrap();

        full_parser.merge_with(parser);
    }
    debug!("Merged {} parsers into one", parsers.len() + 1);
    full_parser.repair_redirects();
    debug!("Repaired redirects");

    (full_parser.content_pages, full_parser.redirects)
}
