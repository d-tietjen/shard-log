use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DEFAULT_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_BLOCK_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
struct Settings {
    input: PathBuf,
    limit_bytes: u64,
    block_bytes: usize,
    codecs: Vec<Codec>,
    report_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Codec {
    Copy,
    Lz4Flex,
    Lz4Native,
    Lz4NativeHighCompression,
    Snappy,
    S2,
    S2Better,
    S2Best,
    MinLzBalanced,
    Flate2,
    Libdeflate,
    ZlibRs,
    Zopfli,
    Brotli,
    Bzip2,
    Xz2,
    LzmaRust2,
    Lzfse,
    LzfseRust,
    ZripFast,
    ZripBest,
    Zstd1,
    Zstd3,
    Zstd9,
}

impl Codec {
    /// The balanced one-gigabyte screen. Slow archival codecs remain available
    /// through `--codecs archive` or `--codecs all`.
    const SCREEN: [Self; 19] = [
        Self::Copy,
        Self::Lz4Flex,
        Self::Lz4Native,
        Self::Lz4NativeHighCompression,
        Self::Snappy,
        Self::S2,
        Self::S2Better,
        Self::MinLzBalanced,
        Self::Flate2,
        Self::Libdeflate,
        Self::ZlibRs,
        Self::Brotli,
        Self::Lzfse,
        Self::LzfseRust,
        Self::ZripFast,
        Self::ZripBest,
        Self::Zstd1,
        Self::Zstd3,
        Self::Zstd9,
    ];

    const ARCHIVE: [Self; 8] = [
        Self::S2Best,
        Self::Zopfli,
        Self::Bzip2,
        Self::Xz2,
        Self::LzmaRust2,
        Self::Brotli,
        Self::ZripBest,
        Self::Zstd9,
    ];

    const ALL: [Self; 24] = [
        Self::Copy,
        Self::Lz4Flex,
        Self::Lz4Native,
        Self::Lz4NativeHighCompression,
        Self::Snappy,
        Self::S2,
        Self::S2Better,
        Self::S2Best,
        Self::MinLzBalanced,
        Self::Flate2,
        Self::Libdeflate,
        Self::ZlibRs,
        Self::Zopfli,
        Self::Brotli,
        Self::Bzip2,
        Self::Xz2,
        Self::LzmaRust2,
        Self::Lzfse,
        Self::LzfseRust,
        Self::ZripFast,
        Self::ZripBest,
        Self::Zstd1,
        Self::Zstd3,
        Self::Zstd9,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Lz4Flex => "lz4_flex",
            Self::Lz4Native => "lz4_native",
            Self::Lz4NativeHighCompression => "lz4_native_hc-9",
            Self::Snappy => "snap",
            Self::S2 => "s2",
            Self::S2Better => "s2_better",
            Self::S2Best => "s2_best",
            Self::MinLzBalanced => "minlz_balanced",
            Self::Flate2 => "deflate-6",
            Self::Libdeflate => "libdeflate-6",
            Self::ZlibRs => "zlib_rs-6",
            Self::Zopfli => "zopfli-5",
            Self::Brotli => "brotli-5",
            Self::Bzip2 => "bzip2-9",
            Self::Xz2 => "xz2-6",
            Self::LzmaRust2 => "lzma_rust2_xz-6",
            Self::Lzfse => "lzfse",
            Self::LzfseRust => "lzfse_rust",
            Self::ZripFast => "zrip-1",
            Self::ZripBest => "zrip-4",
            Self::Zstd1 => "zstd-1",
            Self::Zstd3 => "zstd-3",
            Self::Zstd9 => "zstd-9",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|codec| codec.name() == name)
    }

    fn profile(name: &str) -> Option<Vec<Self>> {
        match name {
            "screen" => Some(Self::SCREEN.to_vec()),
            "archive" => Some(Self::ARCHIVE.to_vec()),
            "all" => Some(Self::ALL.to_vec()),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct CodecResult {
    codec: Codec,
    source_bytes: u64,
    stored_bytes: u64,
    blocks: u64,
    compression_time: Duration,
}

impl CodecResult {
    fn ratio(&self) -> f64 {
        self.source_bytes as f64 / self.stored_bytes.max(1) as f64
    }

    fn mib_per_second(&self) -> f64 {
        let seconds = self.compression_time.as_secs_f64().max(f64::MIN_POSITIVE);
        self.source_bytes as f64 / (1024.0 * 1024.0) / seconds
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let settings = parse_settings()?;
    let mut results = Vec::with_capacity(settings.codecs.len());
    for codec in settings.codecs.iter().copied() {
        results.push(run_codec(&settings, codec)?);
    }
    let report = format_report(&settings, &results);
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
        "usage: shard-telemetry-codec-bench <raw-log-file> [--codecs NAMES|screen|archive|all] [--limit-bytes N] [--block-bytes N] [--report PATH]",
    )?;
    let mut limit_bytes = DEFAULT_LIMIT_BYTES;
    let mut block_bytes = DEFAULT_BLOCK_BYTES;
    let mut codecs = None;
    let mut report_path = None;
    while let Some(flag) = arguments.next() {
        match flag.to_string_lossy().as_ref() {
            "--limit-bytes" => {
                let value = arguments.next().ok_or("--limit-bytes requires a value")?;
                limit_bytes = parse_size(&value.to_string_lossy())?;
            }
            "--block-bytes" => {
                let value = arguments.next().ok_or("--block-bytes requires a value")?;
                block_bytes = usize::try_from(parse_size(&value.to_string_lossy())?)?;
            }
            "--codecs" => {
                let value = arguments
                    .next()
                    .ok_or("--codecs requires comma-separated names")?;
                codecs = Some(parse_codecs(&value.to_string_lossy())?);
            }
            "--report" => {
                report_path = Some(
                    arguments
                        .next()
                        .map(PathBuf::from)
                        .ok_or("--report requires a path")?,
                );
            }
            value => return Err(format!("unknown argument {value}").into()),
        }
    }
    if limit_bytes == 0 || block_bytes == 0 {
        return Err("byte limits must be nonzero".into());
    }
    let codecs = codecs.unwrap_or_else(|| Codec::SCREEN.to_vec());
    if codecs.contains(&Codec::MinLzBalanced) && block_bytes > minlz::minlz::MAX_BLOCK_SIZE {
        return Err(format!(
            "minlz_balanced supports at most {} bytes per block",
            minlz::minlz::MAX_BLOCK_SIZE
        )
        .into());
    }
    Ok(Settings {
        input,
        limit_bytes,
        block_bytes,
        codecs,
        report_path,
    })
}

fn parse_codecs(value: &str) -> Result<Vec<Codec>, Box<dyn Error>> {
    if let Some(profile) = Codec::profile(value.trim()) {
        return Ok(profile);
    }
    let mut codecs = Vec::new();
    for name in value.split(',') {
        let codec = Codec::parse(name.trim()).ok_or_else(|| format!("unknown codec {name:?}"))?;
        if codecs
            .iter()
            .any(|existing: &Codec| existing.name() == codec.name())
        {
            return Err(format!("codec {name:?} appears more than once").into());
        }
        codecs.push(codec);
    }
    if codecs.is_empty() {
        return Err("--codecs cannot be empty".into());
    }
    Ok(codecs)
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

fn run_codec(settings: &Settings, codec: Codec) -> Result<CodecResult, Box<dyn Error>> {
    let mut reader = BufReader::with_capacity(1024 * 1024, File::open(&settings.input)?);
    let mut block = Vec::with_capacity(settings.block_bytes);
    let mut source_bytes = 0u64;
    let mut stored_bytes = 0u64;
    let mut blocks = 0u64;
    let mut compression_time = Duration::ZERO;
    loop {
        let remaining = settings.limit_bytes.saturating_sub(source_bytes);
        if remaining == 0 {
            break;
        }
        let target = settings
            .block_bytes
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let read = read_block(&mut reader, &mut block, target)?;
        if read == 0 {
            break;
        }
        let started = Instant::now();
        let encoded = compress(codec, &block)?;
        compression_time = compression_time.saturating_add(started.elapsed());
        if blocks == 0 {
            let decoded = decompress(codec, &encoded, block.len())?;
            if decoded != block {
                return Err(format!(
                    "{} failed first-block round-trip verification",
                    codec.name()
                )
                .into());
            }
        }
        source_bytes = source_bytes.saturating_add(u64::try_from(read)?);
        stored_bytes = stored_bytes.saturating_add(u64::try_from(encoded.len())?);
        blocks = blocks.saturating_add(1);
    }
    if source_bytes == 0 {
        return Err("input contained no bytes".into());
    }
    Ok(CodecResult {
        codec,
        source_bytes,
        stored_bytes,
        blocks,
        compression_time,
    })
}

fn read_block(
    reader: &mut BufReader<File>,
    block: &mut Vec<u8>,
    target_bytes: usize,
) -> Result<usize, Box<dyn Error>> {
    block.clear();
    block.resize(target_bytes, 0);
    let mut read = 0usize;
    while read < target_bytes {
        let bytes = reader.read(&mut block[read..])?;
        if bytes == 0 {
            break;
        }
        read = read.saturating_add(bytes);
    }
    block.truncate(read);
    Ok(read)
}

fn compress(codec: Codec, mut input: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    match codec {
        Codec::Copy => Ok(input.to_vec()),
        Codec::Lz4Flex => Ok(lz4_flex::block::compress_prepend_size(input)),
        Codec::Lz4Native => Ok(lz4::block::compress(input, None, true)?),
        Codec::Lz4NativeHighCompression => Ok(lz4::block::compress(
            input,
            Some(lz4::block::CompressionMode::HIGHCOMPRESSION(9)),
            true,
        )?),
        Codec::Snappy => Ok(snap::raw::Encoder::new().compress_vec(input)?),
        Codec::S2 => Ok(minlz::s2::encode(input)),
        Codec::S2Better => Ok(minlz::s2::encode_better(input)),
        Codec::S2Best => Ok(minlz::s2::encode_best(input)),
        Codec::MinLzBalanced => Ok(minlz::minlz::compress_level(
            input,
            minlz::minlz::Level::Balanced,
        )?),
        Codec::Flate2 => {
            let mut encoder =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
            encoder.write_all(input)?;
            Ok(encoder.finish()?)
        }
        Codec::Libdeflate => {
            let level = libdeflater::CompressionLvl::new(6)
                .expect("the fixed libdeflate level is supported");
            let mut encoder = libdeflater::Compressor::new(level);
            let mut output = vec![0; encoder.zlib_compress_bound(input.len())];
            let length = encoder.zlib_compress(input, &mut output)?;
            output.truncate(length);
            Ok(output)
        }
        Codec::ZlibRs => {
            let mut output = vec![0; zlib_rs::compress_bound(input.len())];
            let (encoded, status) =
                zlib_rs::compress_slice(&mut output, input, zlib_rs::DeflateConfig::new(6));
            if status != zlib_rs::ReturnCode::Ok {
                return Err(format!("zlib-rs compression returned {status:?}").into());
            }
            Ok(encoded.to_vec())
        }
        Codec::Zopfli => {
            let options = zopfli::Options {
                iteration_count: std::num::NonZeroU64::new(5).expect("nonzero iteration count"),
                ..Default::default()
            };
            let mut output = Vec::new();
            zopfli::compress(options, zopfli::Format::Zlib, input, &mut output)?;
            Ok(output)
        }
        Codec::Brotli => {
            let parameters = brotli::enc::BrotliEncoderParams {
                quality: 5,
                ..Default::default()
            };
            let mut output = Vec::new();
            brotli::BrotliCompress(&mut input, &mut output, &parameters)?;
            Ok(output)
        }
        Codec::Bzip2 => {
            let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::best());
            encoder.write_all(input)?;
            Ok(encoder.finish()?)
        }
        Codec::Xz2 => {
            let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 6);
            encoder.write_all(input)?;
            Ok(encoder.finish()?)
        }
        Codec::LzmaRust2 => {
            let mut encoder =
                lzma_rust2::XzWriter::new(Vec::new(), lzma_rust2::XzOptions::with_preset(6))?;
            encoder.write_all(input)?;
            Ok(encoder.finish()?)
        }
        Codec::Lzfse => {
            let mut output = vec![0; input.len().saturating_add(12)];
            let length = lzfse::encode_buffer(input, &mut output).map_err(|error| {
                std::io::Error::other(format!("lzfse compression failed: {error:?}"))
            })?;
            output.truncate(length);
            Ok(output)
        }
        Codec::LzfseRust => {
            let mut output = Vec::new();
            lzfse_rust::encode_bytes(input, &mut output)?;
            Ok(output)
        }
        Codec::ZripFast => Ok(zrip::compress(input, 1)?),
        Codec::ZripBest => Ok(zrip::compress(input, 4)?),
        Codec::Zstd1 => Ok(zstd::bulk::compress(input, 1)?),
        Codec::Zstd3 => Ok(zstd::bulk::compress(input, 3)?),
        Codec::Zstd9 => Ok(zstd::bulk::compress(input, 9)?),
    }
}

fn decompress(
    codec: Codec,
    mut input: &[u8],
    source_bytes: usize,
) -> Result<Vec<u8>, Box<dyn Error>> {
    match codec {
        Codec::Copy => Ok(input.to_vec()),
        Codec::Lz4Flex => Ok(lz4_flex::block::decompress_size_prepended(input)?),
        Codec::Lz4Native | Codec::Lz4NativeHighCompression => {
            Ok(lz4::block::decompress(input, None)?)
        }
        Codec::Snappy => Ok(snap::raw::Decoder::new().decompress_vec(input)?),
        Codec::S2 | Codec::S2Better | Codec::S2Best => Ok(minlz::s2::decode(input)?),
        Codec::MinLzBalanced => Ok(minlz::minlz::decompress(input)?),
        Codec::Flate2 => {
            let mut decoder = flate2::read::ZlibDecoder::new(input);
            let mut output = Vec::with_capacity(source_bytes);
            decoder.read_to_end(&mut output)?;
            Ok(output)
        }
        Codec::Libdeflate => {
            let mut decoder = libdeflater::Decompressor::new();
            let mut output = vec![0; source_bytes];
            let length = decoder.zlib_decompress(input, &mut output)?;
            output.truncate(length);
            Ok(output)
        }
        Codec::ZlibRs => {
            let mut output = vec![0; source_bytes];
            let (decoded, status) =
                zlib_rs::decompress_slice(&mut output, input, zlib_rs::InflateConfig::default());
            if status != zlib_rs::ReturnCode::Ok {
                return Err(format!("zlib-rs decompression returned {status:?}").into());
            }
            Ok(decoded.to_vec())
        }
        Codec::Zopfli => {
            let mut decoder = flate2::read::ZlibDecoder::new(input);
            let mut output = Vec::with_capacity(source_bytes);
            decoder.read_to_end(&mut output)?;
            Ok(output)
        }
        Codec::Brotli => {
            let mut output = Vec::with_capacity(source_bytes);
            brotli::BrotliDecompress(&mut input, &mut output)?;
            Ok(output)
        }
        Codec::Bzip2 => {
            let mut decoder = bzip2::read::BzDecoder::new(input);
            let mut output = Vec::with_capacity(source_bytes);
            decoder.read_to_end(&mut output)?;
            Ok(output)
        }
        Codec::Xz2 => {
            let mut decoder = xz2::read::XzDecoder::new(input);
            let mut output = Vec::with_capacity(source_bytes);
            decoder.read_to_end(&mut output)?;
            Ok(output)
        }
        Codec::LzmaRust2 => {
            let mut decoder = lzma_rust2::XzReader::new(input, false);
            let mut output = Vec::with_capacity(source_bytes);
            decoder.read_to_end(&mut output)?;
            Ok(output)
        }
        Codec::Lzfse => {
            let mut output = vec![0; source_bytes.saturating_add(1)];
            let length = lzfse::decode_buffer(input, &mut output).map_err(|error| {
                std::io::Error::other(format!("lzfse decompression failed: {error:?}"))
            })?;
            output.truncate(length);
            Ok(output)
        }
        Codec::LzfseRust => {
            let mut output = Vec::with_capacity(source_bytes);
            lzfse_rust::decode_bytes(input, &mut output)?;
            Ok(output)
        }
        Codec::ZripFast | Codec::ZripBest => Ok(zrip::decompress(input)?),
        Codec::Zstd1 | Codec::Zstd3 | Codec::Zstd9 => {
            Ok(zstd::bulk::decompress(input, source_bytes)?)
        }
    }
}

fn format_report(settings: &Settings, results: &[CodecResult]) -> String {
    let mut report = String::from("shard-telemetry codec compression benchmark\n");
    report.push_str(&format!("input: {}\n", settings.input.display()));
    report.push_str(&format!("block target: {}\n", settings.block_bytes));
    report.push_str(&format!(
        "codecs: {}\n",
        settings
            .codecs
            .iter()
            .map(|codec| codec.name())
            .collect::<Vec<_>>()
            .join(",")
    ));
    report.push_str("codec,source_bytes,stored_bytes,ratio,retained_percent,compression_seconds,compression_mib_per_second,blocks\n");
    for result in results {
        let retained_percent = result.stored_bytes as f64 * 100.0 / result.source_bytes as f64;
        report.push_str(&format!(
            "{},{},{},{:.4},{:.4},{:.6},{:.2},{}\n",
            result.codec.name(),
            result.source_bytes,
            result.stored_bytes,
            result.ratio(),
            retained_percent,
            result.compression_time.as_secs_f64(),
            result.mib_per_second(),
            result.blocks,
        ));
    }
    report.push_str("note: every codec uses independent blocks and passes first-block round-trip verification; codec throughput excludes input read time. `screen` omits slow archival candidates, while `all` includes every configured engine.\n");
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_codec_round_trips_repeated_log_content() {
        let input = b"{\"log\":\"ERROR retry=42 request_id=abc\"}\n".repeat(1024);
        for codec in Codec::ALL {
            let compressed = compress(codec, &input).expect("compresses");
            assert_eq!(
                decompress(codec, &compressed, input.len()).expect("decompresses"),
                input,
                "{}",
                codec.name()
            );
        }
    }

    #[test]
    fn parses_binary_and_iec_byte_limits() {
        assert_eq!(parse_size("10").expect("bytes"), 10);
        assert_eq!(parse_size("2KiB").expect("kib"), 2048);
        assert_eq!(parse_size("3MiB").expect("mib"), 3 * 1024 * 1024);
    }

    #[test]
    fn accepts_a_nonduplicated_codec_subset() {
        let codecs = parse_codecs("lz4_flex,zstd-1,zstd-9").expect("parses");
        assert_eq!(
            codecs.iter().map(|codec| codec.name()).collect::<Vec<_>>(),
            ["lz4_flex", "zstd-1", "zstd-9"]
        );
        assert!(parse_codecs("lz4_flex,lz4_flex").is_err());
    }

    #[test]
    fn expands_named_codec_profiles() {
        assert_eq!(parse_codecs("screen").expect("screen"), Codec::SCREEN);
        assert_eq!(parse_codecs("archive").expect("archive"), Codec::ARCHIVE);
        assert_eq!(parse_codecs("all").expect("all"), Codec::ALL);
    }
}
