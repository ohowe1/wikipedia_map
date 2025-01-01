use std::thread;
use log::debug;
use parse_lib::article_parser::{pool};

fn main() {
    env_logger::init();
    let path = "data/a.xml";
    // let path = "data/enwiki-latest-pages-articles-multistream.xml";

    let threads = (thread::available_parallelism().unwrap().get() - 1) as u32;
    debug!("Running on {} threads", threads);

    let (content_pages, redirects) = pool::process_file(path, threads);
    
    debug!("Read {} content pages and {} redirects", content_pages.len(), redirects.len());
    debug!("Done. Writing to file.");
    
    std::fs::write("content_pages.data", bincode::serialize(&content_pages).unwrap()).expect("Unable to write content pages file");
    std::fs::write("redirects.data", bincode::serialize(&redirects).unwrap()).expect("Unable to write redirects file");
}
