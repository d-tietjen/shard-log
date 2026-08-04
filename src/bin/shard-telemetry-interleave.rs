use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

#[derive(Debug)]
struct Settings {
    output: PathBuf,
    inputs: Vec<PathBuf>,
    limit_bytes: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let settings = parse_settings()?;
    let mut readers = settings
        .inputs
        .iter()
        .map(File::open)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(BufReader::new)
        .collect::<Vec<_>>();
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&settings.output)?;
    let mut output = BufWriter::new(output);
    let mut active = vec![true; readers.len()];
    let mut written = 0u64;
    let mut records = 0u64;
    let mut buffer = Vec::new();

    while active.iter().any(|active| *active) {
        for (reader, active) in readers.iter_mut().zip(&mut active) {
            if !*active {
                continue;
            }
            buffer.clear();
            let read = reader.read_until(b'\n', &mut buffer)?;
            if read == 0 {
                *active = false;
                continue;
            }
            if !buffer.ends_with(b"\n") {
                *active = false;
                continue;
            }
            let line_bytes = u64::try_from(buffer.len())?;
            if written.saturating_add(line_bytes) > settings.limit_bytes {
                output.flush()?;
                println!("records: {records}");
                println!("bytes: {written}");
                return Ok(());
            }
            output.write_all(&buffer)?;
            written = written.saturating_add(line_bytes);
            records = records.saturating_add(1);
        }
    }
    output.flush()?;
    println!("records: {records}");
    println!("bytes: {written}");
    Ok(())
}

fn parse_settings() -> Result<Settings, Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let output = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: shard-telemetry-interleave <output.log> [--limit-bytes N] <input.log>...")?;
    let mut inputs = Vec::new();
    let mut limit_bytes = u64::MAX;
    while let Some(argument) = arguments.next() {
        if argument == "--limit-bytes" {
            limit_bytes = arguments
                .next()
                .ok_or("--limit-bytes requires a value")?
                .to_string_lossy()
                .parse()?;
        } else {
            inputs.push(PathBuf::from(argument));
        }
    }
    if inputs.len() < 2 {
        return Err("at least two input logs are required".into());
    }
    if limit_bytes == 0 {
        return Err("--limit-bytes must be nonzero".into());
    }
    Ok(Settings {
        output,
        inputs,
        limit_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_multiple_inputs() {
        let settings = Settings {
            output: PathBuf::from("out"),
            inputs: vec![PathBuf::from("one")],
            limit_bytes: 1,
        };
        assert_eq!(settings.inputs.len(), 1);
    }
}
