extern crate csv;
extern crate glob;
extern crate rayon;
extern crate chrono;

use rayon::prelude::*;
use std::env;
use std::fs::File;
use std::io::Read;
use std::str::FromStr;
use std::time::Instant;
use chrono::NaiveDateTime;
use chrono::Datelike;

fn compute_file(content: &str) -> [f64; 12] {
    let rdr = csv::ReaderBuilder::new().from_reader(content.as_bytes());
    let ok_records = rdr.into_records().filter_map(Result::ok);
    let mut res = [0.0; 12];
    ok_records
        .filter(|x| f64::from_str(x.get(4).expect("Format")).unwrap_or(0.0) > 1.0) // miles
        .filter(|x| f64::from_str(x.get(3).expect("Format")).unwrap_or(0.0) > 60.0) // time s
        .for_each(|x| {
            let date = NaiveDateTime::parse_from_str(x.get(1).expect("Format"), "%Y-%m-%d %H:%M:%S").expect("invalid time");
            if x.get(14).expect("Format") == "Cash" {
                res[date.month() as usize - 1] = f64::from_str(x.get(13).expect("Format")).unwrap_or(0.0);
            }
        });
    res
}

fn records_for_path(path: &str) -> impl ParallelIterator<Item=csv::StringRecord> {
    let rdr = csv::ReaderBuilder::new().from_path(path).unwrap();
    rdr.into_records().filter_map(Result::ok).par_bridge()
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
    println!(
        "Elapsed time loading: {}s {}ms",
        now.elapsed().as_secs(),
        now.elapsed().subsec_millis()
    );
    let now = Instant::now();
    let files = get_file_paths();
    
    let res = files.par_iter()
        .map(|content| records_for_path(&content[..]))
        .flatten()
        .filter(|x| f64::from_str(x.get(4).expect("Format")).unwrap_or(0.0) > 1.0) // miles
        .filter(|x| f64::from_str(x.get(3).expect("Format")).unwrap_or(0.0) > 60.0) // time s
        .filter(|x| x.get(14).expect("Format") == "Cash") // time s
        .map(|x| {
            let date = NaiveDateTime::parse_from_str(x.get(1).expect("Format"), "%Y-%m-%d %H:%M:%S").expect("invalid time");
            (date.month() - 1, f64::from_str(x.get(13).expect("Format")).unwrap_or(0.0))
        })
        .fold(|| [0.0; 12], |mut part, (month, a)| {
            part[month as usize] = part[month as usize] + a;
            part
        }).
        reduce(|| [0.0; 12], |a, b| {
            let mut res = [0.0; 12];
            for i in 0..12 {
                res[i] = a[i] + b[i];
            }
            res
        });
            
    println!(
        "Elapsed time: {}s {}ms",
        now.elapsed().as_secs(),
        now.elapsed().subsec_millis()
    );
    println!("result: {:?}", res);
}

fn get_file_paths() -> Vec<String> {
    let file_path = env::args().nth(1).expect("Expect file path as argument");
    glob::glob(&file_path[..])
        .expect("Glob should succeed")
        .map(|m| format!("{}", m.expect("Should be path").display()))
        .collect()
}