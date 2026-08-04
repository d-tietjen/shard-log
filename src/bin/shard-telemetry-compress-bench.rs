use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

const DEFAULT_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_BLOCK_BYTES: usize = 8 * 1024 * 1024;
const DICTIONARY_SAMPLE_BYTES: usize = 64 * 1024 * 1024;
const DICTIONARY_BYTES: usize = 112 * 1024;
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug)]
struct Settings {
    input: PathBuf,
    stdin_spool_path: Option<PathBuf>,
    report_path: Option<PathBuf>,
    limit_bytes: u64,
    block_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct BlockCompression {
    source_bytes: u64,
    stored_bytes: u64,
    blocks: u64,
}

#[derive(Debug, Clone, Copy)]
struct TemplateCompression {
    source_bytes: u64,
    record_bytes: u64,
    template_bytes: u64,
    index_bytes: u64,
    template_count: u64,
    index_terms: u64,
    blocks: u64,
}

impl TemplateCompression {
    fn payload_bytes(self) -> u64 {
        self.record_bytes + self.template_bytes
    }

    fn total_bytes(self) -> u64 {
        self.payload_bytes() + self.index_bytes
    }
}

struct TemplateEncoder {
    template_ids: HashMap<Vec<u8>, u32>,
    template_terms: Vec<Vec<Vec<u8>>>,
    encoded_templates: Vec<u8>,
}

impl TemplateEncoder {
    fn new() -> Self {
        Self {
            template_ids: HashMap::new(),
            template_terms: Vec::new(),
            encoded_templates: Vec::new(),
        }
    }

    fn encode_record(
        &mut self,
        line: &[u8],
        output: &mut Vec<u8>,
        block_terms: &mut HashSet<Vec<u8>>,
    ) {
        let (template, values) = split_template(line);
        let template_id = if let Some(template_id) = self.template_ids.get(&template) {
            *template_id
        } else {
            let template_id = u32::try_from(self.template_ids.len())
                .expect("template corpus cannot contain more than u32 templates");
            write_varint(
                u64::try_from(template.len()).expect("template length fits u64"),
                &mut self.encoded_templates,
            );
            self.encoded_templates.extend_from_slice(&template);
            self.template_terms.push(extract_terms(&template));
            self.template_ids.insert(template, template_id);
            template_id
        };
        write_varint(u64::from(template_id), output);
        write_varint(
            u64::try_from(values.len()).expect("value count fits u64"),
            output,
        );
        for value in values {
            write_varint(
                u64::try_from(value.len()).expect("value length fits u64"),
                output,
            );
            output.extend_from_slice(value);
        }
        for term in &self.template_terms[template_id as usize] {
            block_terms.insert(term.clone());
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut settings = parse_settings()?;
    materialize_stdin(&mut settings)?;
    let dictionary = train_dictionary(&settings)?;
    let raw = compress_raw(&settings, &[])?;
    let raw_with_dictionary = compress_raw(&settings, &dictionary)?;
    let template = compress_template(&settings)?;
    let mut report = String::from("shard-telemetry real-log compression benchmark\n");
    report.push_str(&format!("input: {}\n", settings.input.display()));
    report.push_str(&format!("source bytes: {}\n", raw.source_bytes));
    report.push_str(&format!("block target: {}\n", settings.block_bytes));
    report.push_str(&format!("dictionary bytes: {}\n", dictionary.len()));
    report.push_str(&result_line(
        "zstd block payload",
        raw.stored_bytes,
        raw.source_bytes,
    ));
    report.push_str(&result_line(
        "zstd + trained dictionary (dictionary included)",
        raw_with_dictionary
            .stored_bytes
            .saturating_add(u64::try_from(dictionary.len())?),
        raw_with_dictionary.source_bytes,
    ));
    report.push_str(&result_line(
        "template column payload",
        template.payload_bytes(),
        template.source_bytes,
    ));
    report.push_str(&result_line(
        "template payload + block-term index",
        template.total_bytes(),
        template.source_bytes,
    ));
    report.push_str(&format!("raw blocks: {}\n", raw.blocks));
    report.push_str(&format!("template blocks: {}\n", template.blocks));
    report.push_str(&format!("unique templates: {}\n", template.template_count));
    report.push_str(&format!("indexed static terms: {}\n", template.index_terms));
    report.push_str(&format!(
        "template components: records={} templates={} index={}\n",
        template.record_bytes, template.template_bytes, template.index_bytes
    ));
    report.push_str(
        "note: the final total includes a compressed block-level static-term index; it excludes\nper-record postings for high-cardinality variable values, which belong in metadata columns."
    );
    if let Some(path) = &settings.report_path {
        let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
        output.write_all(report.as_bytes())?;
        output.flush()?;
    }
    print!("{report}");
    Ok(())
}

fn parse_settings() -> Result<Settings, Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let input = arguments.next().map(PathBuf::from).ok_or(
        "usage: shard-telemetry-compress-bench <raw-log-file|-> [--spool-stdin-to PATH] [--report PATH] [--limit-bytes N] [--block-bytes N]",
    )?;
    let mut limit_bytes = DEFAULT_LIMIT_BYTES;
    let mut block_bytes = DEFAULT_BLOCK_BYTES;
    let mut stdin_spool_path = None;
    let mut report_path = None;
    while let Some(flag) = arguments.next() {
        match flag.to_string_lossy().as_ref() {
            "--report" => {
                report_path = Some(
                    arguments
                        .next()
                        .map(PathBuf::from)
                        .ok_or("--report requires a path")?,
                );
            }
            "--spool-stdin-to" => {
                stdin_spool_path = Some(
                    arguments
                        .next()
                        .map(PathBuf::from)
                        .ok_or("--spool-stdin-to requires a path")?,
                );
            }
            "--limit-bytes" => {
                let value = arguments.next().ok_or("--limit-bytes requires a value")?;
                limit_bytes = parse_size(&value.to_string_lossy())?;
            }
            "--block-bytes" => {
                let value = arguments.next().ok_or("--block-bytes requires a value")?;
                block_bytes = usize::try_from(parse_size(&value.to_string_lossy())?)?;
            }
            value => return Err(format!("unknown argument {value}").into()),
        }
    }
    if limit_bytes == 0 || block_bytes == 0 {
        return Err("byte limits must be nonzero".into());
    }
    let reads_stdin = input.as_os_str() == "-";
    if reads_stdin && stdin_spool_path.is_none() {
        return Err(
            "stdin requires --spool-stdin-to PATH so every layout reads identical input".into(),
        );
    }
    if !reads_stdin && stdin_spool_path.is_some() {
        return Err("--spool-stdin-to is valid only when input is -".into());
    }
    Ok(Settings {
        input,
        stdin_spool_path,
        report_path,
        limit_bytes,
        block_bytes,
    })
}

fn materialize_stdin(settings: &mut Settings) -> Result<(), Box<dyn Error>> {
    if settings.input.as_os_str() != "-" {
        return Ok(());
    }
    let path = settings
        .stdin_spool_path
        .take()
        .expect("stdin path was validated during argument parsing");
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    let stdin = std::io::stdin();
    let mut input = BufReader::with_capacity(1024 * 1024, stdin.lock());
    let mut line = Vec::new();
    let mut copied = 0u64;
    while input.read_until(b'\n', &mut line)? != 0 {
        let line_bytes = u64::try_from(line.len())?;
        if copied.saturating_add(line_bytes) > settings.limit_bytes {
            break;
        }
        output.write_all(&line)?;
        copied = copied.saturating_add(line_bytes);
        line.clear();
    }
    output.flush()?;
    if copied == 0 {
        return Err("stdin did not contain a complete line within the byte limit".into());
    }
    println!("spooled {copied} stdin bytes to {}", path.display());
    settings.input = path;
    Ok(())
}

fn parse_size(value: &str) -> Result<u64, Box<dyn Error>> {
    let normalized = value.trim().to_ascii_lowercase();
    let (number, multiplier) = if let Some(number) = normalized.strip_suffix("gib") {
        (number, 1024_u64.pow(3))
    } else if let Some(number) = normalized.strip_suffix("mib") {
        (number, 1024_u64.pow(2))
    } else if let Some(number) = normalized.strip_suffix("kib") {
        (number, 1024)
    } else {
        (normalized.as_str(), 1)
    };
    Ok(number.parse::<u64>()?.saturating_mul(multiplier))
}

fn train_dictionary(settings: &Settings) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut reader = open_reader(&settings.input)?;
    let mut samples = Vec::new();
    let mut line = Vec::new();
    let mut sampled = 0usize;
    while sampled < DICTIONARY_SAMPLE_BYTES {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if line.is_empty() {
            continue;
        }
        sampled = sampled.saturating_add(line.len());
        samples.push(line.clone());
    }
    if samples.len() < 8 {
        return Err("input has too few lines to train a zstd dictionary".into());
    }
    Ok(zstd::dict::from_samples(&samples, DICTIONARY_BYTES)?)
}

fn compress_raw(
    settings: &Settings,
    dictionary: &[u8],
) -> Result<BlockCompression, Box<dyn Error>> {
    let mut reader = open_reader(&settings.input)?;
    let mut compressor = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, dictionary)?;
    let mut source_bytes = 0u64;
    let mut stored_bytes = 0u64;
    let mut blocks = 0u64;
    let mut block = Vec::with_capacity(settings.block_bytes);
    let mut line = Vec::new();
    while read_limited_line(
        &mut reader,
        &mut line,
        &mut source_bytes,
        settings.limit_bytes,
    )? {
        block.extend_from_slice(&line);
        if block.len() >= settings.block_bytes {
            stored_bytes =
                stored_bytes.saturating_add(u64::try_from(compressor.compress(&block)?.len())?);
            blocks = blocks.saturating_add(1);
            block.clear();
        }
    }
    if !block.is_empty() {
        stored_bytes =
            stored_bytes.saturating_add(u64::try_from(compressor.compress(&block)?.len())?);
        blocks = blocks.saturating_add(1);
    }
    Ok(BlockCompression {
        source_bytes,
        stored_bytes,
        blocks,
    })
}

fn compress_template(settings: &Settings) -> Result<TemplateCompression, Box<dyn Error>> {
    let mut reader = open_reader(&settings.input)?;
    let mut encoder = TemplateEncoder::new();
    let mut compressor = zstd::bulk::Compressor::new(ZSTD_LEVEL)?;
    let mut source_bytes = 0u64;
    let mut record_bytes = 0u64;
    let mut blocks = 0u64;
    let mut block_id = 0u32;
    let mut record_block = Vec::with_capacity(settings.block_bytes);
    let mut block_terms = HashSet::new();
    let mut term_blocks: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
    let mut line = Vec::new();
    while read_limited_line(
        &mut reader,
        &mut line,
        &mut source_bytes,
        settings.limit_bytes,
    )? {
        encoder.encode_record(&line, &mut record_block, &mut block_terms);
        if record_block.len() >= settings.block_bytes {
            record_bytes = record_bytes
                .saturating_add(u64::try_from(compressor.compress(&record_block)?.len())?);
            flush_terms(&mut block_terms, &mut term_blocks, block_id);
            record_block.clear();
            blocks = blocks.saturating_add(1);
            block_id = block_id
                .checked_add(1)
                .ok_or("template block identifier overflow")?;
        }
    }
    if !record_block.is_empty() {
        record_bytes =
            record_bytes.saturating_add(u64::try_from(compressor.compress(&record_block)?.len())?);
        flush_terms(&mut block_terms, &mut term_blocks, block_id);
        blocks = blocks.saturating_add(1);
    }
    let template_bytes =
        u64::try_from(zstd::bulk::compress(&encoder.encoded_templates, ZSTD_LEVEL)?.len())?;
    let index_terms = u64::try_from(term_blocks.len())?;
    let index = encode_term_index(term_blocks);
    let index_bytes = u64::try_from(zstd::bulk::compress(&index, ZSTD_LEVEL)?.len())?;
    Ok(TemplateCompression {
        source_bytes,
        record_bytes,
        template_bytes,
        index_bytes,
        template_count: u64::try_from(encoder.template_ids.len())?,
        index_terms,
        blocks,
    })
}

fn open_reader(path: &Path) -> Result<BufReader<File>, Box<dyn Error>> {
    Ok(BufReader::with_capacity(1024 * 1024, File::open(path)?))
}

fn read_limited_line(
    reader: &mut BufReader<File>,
    line: &mut Vec<u8>,
    source_bytes: &mut u64,
    limit_bytes: u64,
) -> Result<bool, Box<dyn Error>> {
    line.clear();
    if reader.read_until(b'\n', line)? == 0 {
        return Ok(false);
    }
    let line_bytes = u64::try_from(line.len())?;
    if source_bytes.saturating_add(line_bytes) > limit_bytes {
        return Ok(false);
    }
    *source_bytes = source_bytes.saturating_add(line_bytes);
    Ok(true)
}

fn flush_terms(
    block_terms: &mut HashSet<Vec<u8>>,
    term_blocks: &mut HashMap<Vec<u8>, Vec<u32>>,
    block_id: u32,
) {
    for term in block_terms.drain() {
        term_blocks.entry(term).or_default().push(block_id);
    }
}

fn split_template(line: &[u8]) -> (Vec<u8>, Vec<&[u8]>) {
    let mut template = Vec::with_capacity(line.len());
    let mut values = Vec::new();
    let mut cursor = 0usize;
    while cursor < line.len() {
        let start = cursor;
        let whitespace = line[cursor].is_ascii_whitespace();
        while cursor < line.len() && line[cursor].is_ascii_whitespace() == whitespace {
            cursor += 1;
        }
        let part = &line[start..cursor];
        if !whitespace && is_variable(part) {
            template.push(0x1e);
            values.push(part);
        } else {
            template.extend_from_slice(part);
        }
    }
    (template, values)
}

fn is_variable(token: &[u8]) -> bool {
    token.iter().any(|byte| byte.is_ascii_digit())
}

fn extract_terms(template: &[u8]) -> Vec<Vec<u8>> {
    let mut terms = Vec::new();
    let mut cursor = 0usize;
    while cursor < template.len() {
        while cursor < template.len() && !template[cursor].is_ascii_alphabetic() {
            cursor += 1;
        }
        let start = cursor;
        while cursor < template.len() && template[cursor].is_ascii_alphanumeric() {
            cursor += 1;
        }
        if cursor > start {
            let mut term = template[start..cursor].to_vec();
            term.make_ascii_lowercase();
            terms.push(term);
        }
    }
    terms.sort_unstable();
    terms.dedup();
    terms
}

fn encode_term_index(mut term_blocks: HashMap<Vec<u8>, Vec<u32>>) -> Vec<u8> {
    let mut entries = term_blocks.drain().collect::<Vec<_>>();
    entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let mut encoded = Vec::new();
    for (term, mut blocks) in entries {
        blocks.sort_unstable();
        blocks.dedup();
        write_varint(
            u64::try_from(term.len()).expect("term length fits u64"),
            &mut encoded,
        );
        encoded.extend_from_slice(&term);
        write_varint(
            u64::try_from(blocks.len()).expect("block count fits u64"),
            &mut encoded,
        );
        let mut previous = 0u32;
        for (index, block_id) in blocks.into_iter().enumerate() {
            let delta = if index == 0 {
                block_id
            } else {
                block_id.saturating_sub(previous)
            };
            write_varint(u64::from(delta), &mut encoded);
            previous = block_id;
        }
    }
    encoded
}

fn write_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn result_line(label: &str, stored_bytes: u64, source_bytes: u64) -> String {
    let ratio = source_bytes as f64 / stored_bytes.max(1) as f64;
    let percent = stored_bytes as f64 * 100.0 / source_bytes.max(1) as f64;
    format!("{label}: {stored_bytes} bytes ({ratio:.2}x, {percent:.2}% of source)\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_encoding_replaces_only_variable_tokens() {
        let (template, values) = split_template(b"INFO block=blk_123 retry count=4\n");
        assert_eq!(template, b"INFO \x1e retry \x1e\n");
        assert_eq!(
            values,
            vec![b"block=blk_123".as_slice(), b"count=4".as_slice()]
        );
    }

    #[test]
    fn static_terms_ignore_variable_values() {
        assert_eq!(
            extract_terms(b"ERROR \x1e failed"),
            vec![b"error".to_vec(), b"failed".to_vec()]
        );
    }
}
