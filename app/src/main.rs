use std::io::Write;
use std::thread;
use log::debug;
use parse_lib::article_parser::{pool};

fn main() {
    env_logger::init();
    let path = "data/enwiki-latest-pages-articles-multistream.xml";

    let (content_pages, redirects) = read_wikipedia_xml(path);
    debug!("Done. Writing to file.");

    std::fs::write("../../parsed/content_pages.data", bincode::serialize(&content_pages).unwrap()).expect("Unable to write content pages file");
    std::fs::write("../../parsed/redirects.data", bincode::serialize(&redirects).unwrap()).expect("Unable to write redirects file");

    to_graph_csv("graph.csv", &content_pages);
}

fn read_wikipedia_xml(file_path: &str) -> (Vec<parse_lib::article_parser::ContentPage>, std::collections::HashMap<u64, u64>) {
    let threads = (thread::available_parallelism().unwrap().get() - 1) as u64;
    debug!("Running on {} threads", threads);
    
    let (content_pages, redirects) = pool::process_file(file_path, threads);
    
    debug!("Read {} content pages and {} redirects", content_pages.len(), redirects.len());
    
    (content_pages, redirects)
}

fn to_graph_csv(csv_file: &str, content_pages: &[parse_lib::article_parser::ContentPage]) {
    let file = std::fs::File::create(csv_file).expect("Unable to create CSV file");
    let mut buf_writer = std::io::BufWriter::new(file);

    buf_writer.write("source,target\n".as_ref()).expect("Unable to write to CSV file");
    for page in content_pages {
        for link in &page.links {
            buf_writer.write(format!("{},{}\n", page.title_hash, link).as_ref()).expect("Unable to write to CSV file");
        }
    }
}
