



pub mod message;
pub mod log;

pub type BbError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type BbResult<T> = Result<T, BbError>;



#[test]
fn testcase() {
    println!("debug");
}   