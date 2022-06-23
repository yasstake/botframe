pub mod log;
pub mod market;
pub mod message;

#[cfg(test)]
pub mod test;
#[cfg(test)]
pub mod testdata;

/*
pub type BbError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type BbResult<T> = Result<T, BbError>;
*/

#[test]
fn testcase() {
    println!("debug");
}
