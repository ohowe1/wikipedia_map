use std::thread;
use log::debug;
use parse_lib::article_parser::pool::Pool;

fn main() {
    env_logger::init();
    let path = "data/a.xml";

    let threads = thread::available_parallelism().unwrap().get() - 1;
    debug!("Running on {} threads", threads);
    let mut pool = Pool::new(threads as u32);

    pool.process_file(path);
}
