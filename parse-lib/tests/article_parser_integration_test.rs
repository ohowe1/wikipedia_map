use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use log::debug;
use parse_lib::article_parser::ArticleParser;
use parse_lib::util::test_init;

#[test]
fn parse_xml_test() {
    test_init();
    debug!("Parse XML Test");

    let mut reader = BufReader::new(File::open(PathBuf::from("tests").join("data").join("test_data.xml")).expect("Failed to read data file"));
    let mut parser = ArticleParser::new();
    
    parser.parse_xml(&mut reader, None, None);
    let title_to_hash: HashMap<String, u64> = HashMap::from_iter(parser.content_pages().iter().map(|page| (page.title.clone(), page.title_hash)));

    // Content pages
    assert_eq!(parser.content_pages().len(), 3);
    
    assert_eq!(parser.content_pages()[0].title, "Article 1");
    assert_eq!(parser.content_pages()[0].links, vec![title_to_hash["Article 2"], title_to_hash["Article 2"], title_to_hash["Article 3"]]);

    assert_eq!(parser.content_pages()[1].title, "Article 2");
    assert_eq!(parser.content_pages()[1].links, vec![title_to_hash["Article 1"]]);

    assert_eq!(parser.content_pages()[2].title, "Article 3");
    assert_eq!(parser.content_pages()[2].links, vec![]);
    
    // Redirects
    assert_eq!(parser.redirects().len(), 1);
    assert_eq!(parser.redirects().values().next().unwrap().clone(), title_to_hash["Article 1"]);
}

#[test]
fn parse_xml_bounded() {
    test_init();
    debug!("Parse XML Bounded Test");

    let mut reader = BufReader::new(File::open(PathBuf::from("tests").join("data").join("test_data.xml")).expect("Failed to read data file"));
    
    // seek to "vision" on line 6
    reader.seek(SeekFrom::Start(135)).expect("Failed to seek file.");
    let mut parser = ArticleParser::new();

    // go till end of <text> on line 23.
    parser.parse_xml(&mut reader, Some(587), None);

    // Content pages
    assert_eq!(parser.content_pages().len(), 2);
    assert_eq!(parser.content_pages()[0].title, "Article 1");
    assert_eq!(parser.content_pages()[0].links.len(), 3);
    assert_eq!(parser.content_pages()[1].title, "Article 2");
    assert_eq!(parser.content_pages()[1].links.len(), 1);

    // Redirects
    assert_eq!(parser.redirects().len(), 0);
}
