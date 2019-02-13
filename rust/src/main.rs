extern crate csv;
extern crate glob;
extern crate rayon;

use rayon::prelude::*;
use std::env;
use std::fs::File;
use std::io::Read;
use std::str::FromStr;
use std::time::Instant;

fn count(contents: &str) -> f64 {
    let rdr = csv::ReaderBuilder::new().from_reader(contents.as_bytes());
    let ok_records = rdr.into_records().filter_map(Result::ok);
    ok_records.fold(0.0, |sum, val| {
        sum + if val.get(14).expect("Wrong format") != "Cash" {
            0.0
        } else {
            f64::from_str(val.get(13).expect("Wrong format")).unwrap_or(0.0)
        }
    })
}

fn read_file(file: &str) -> String {
    println!("Reading file {}", file);
    let mut contents = String::new();
    let mut file = File::open(file).expect("file not found");
    file.read_to_string(&mut contents)
        .expect("something went wrong reading the file");
    contents
}

fn main() {
    let now = Instant::now();
    let file_path = env::args().nth(1).expect("Expect file path as argument");
    let files: Vec<String> = glob::glob(&file_path[..])
        .expect("Glob should succeed")
        .map(|m| format!("{}", m.expect("Should be path").display()))
        .map(|file| read_file(&file[..]))
        .collect();
    println!(
        "Elapsed time loading: {}s {}ms",
        now.elapsed().as_secs(),
        now.elapsed().subsec_millis()
    );
    let now = Instant::now();
    let res: f64 = files.par_iter().map(|contents| count(&contents[..])).sum();
    println!(
        "Elapsed time: {}s {}ms",
        now.elapsed().as_secs(),
        now.elapsed().subsec_millis()
    );
    println!("result: {}", res);
}
