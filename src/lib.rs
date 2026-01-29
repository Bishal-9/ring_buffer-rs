const CACHE_LINE_SIZE: usize = 64;

mod core; // NOT public → crate-internal only
mod utility;

pub mod mpmc;
pub mod mpsc;
pub mod spmc;
pub mod spsc;
