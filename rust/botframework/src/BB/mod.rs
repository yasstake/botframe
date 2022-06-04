pub mod log;
pub mod market;
pub mod message;

pub type BbError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type BbResult<T> = Result<T, BbError>;

#[test]
fn testcase() {
    println!("debug");
}
