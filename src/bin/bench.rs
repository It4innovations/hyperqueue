use std::process::Command;
use std::time::Instant;

fn main() {
    // Populate page tables
    let mut v = vec![];
    for i in 0..1024 * 1024 * 1024 {
        v.push(i * i + 1);
    }

    let start = Instant::now();

    for _ in 0..100 {
        Command::new("sleep").arg("0").output().unwrap();
    }

    println!("{} ms", start.elapsed().as_millis());
    println!("{}", v[1000] + v[11111]);
}
