use crate::article_parser::ArticleParser;

struct Pool {
    parsers: Vec<ArticleParser>,
}

impl Pool {
    fn new(pool_size: u32) -> Self {
        let mut pool = Self {
            parsers: Vec::new(),
        };
        
        pool.fill_pool(pool_size);
        pool
    }
    
    fn fill_pool(&mut self, pool_size: u32) {
        while self.parsers.len() < pool_size as usize {
            self.parsers.push(ArticleParser::new());
        }
    }
}
