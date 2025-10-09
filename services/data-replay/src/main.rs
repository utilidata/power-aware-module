use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use prost::Message;
use protobuf_rs::utilidata::karman::bibimbap::v1::{
    composite_joined_calculations_wrapper::DataProduct, CompositeCalculations,
    CompositeJoinedCalculations, CompositeJoinedCalculationsWrapper, CompositeTwoPhaseCalculations,
    PowerCalculations, Provenance, WaveformCalculations,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zeromq::{Socket, SocketSend};

#[derive(Debug, Deserialize)]
struct CsvRow {
    time: i64,  // Milliseconds since epoch
    stream_name: String,
    phase: String,
    rms_voltage: f32,
    dc_offset_voltage: f32,
    rms_current: f32,
    dc_offset_current: f32,
    real_power: f32,
    apparent_power: f32,
    reactive_power: f32,
    power_factor: f32,
    sequence_number: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let file_path = env::var("FILE").unwrap_or_else(|_| "/datasets/sample1-b200-no-powercap.csv".to_string());
    let pub_addr = env::var("PUB").unwrap_or_else(|_| "tcp://0.0.0.0:5557".to_string());
    let rate_hz: f64 = env::var("RATE_HZ")
        .unwrap_or_else(|_| "60".to_string())
        .parse()
        .context("Invalid RATE_HZ")?;
    let topic = env::var("TOPIC").unwrap_or_default();

    log::info!("Reading dataset from: {}", file_path);
    
    // Read and parse CSV
    let file = File::open(&file_path).context("Could not open dataset file")?;
    let mut rdr = csv::Reader::from_reader(file);
    
    // Group rows by timestamp into frames (each timestamp has 6 rows: 3 streams × 2 phases)
    let mut frames = Vec::new();
    let mut current_frame: HashMap<String, (Option<CsvRow>, Option<CsvRow>)> = HashMap::new();
    let mut last_timestamp: Option<i64> = None;
    let mut frame_count = 0u64;
    
    for result in rdr.deserialize() {
        let row: CsvRow = result.context("Failed to parse CSV row")?;
        
        // New timestamp = new frame (timestamps are in milliseconds)
        if let Some(last) = last_timestamp {
            if row.time != last {
                if !current_frame.is_empty() {
                    frames.push(build_frame(&current_frame, frame_count)?);
                    frame_count += 1;
                    current_frame.clear();
                }
            }
        }
        last_timestamp = Some(row.time);
        
        // Group phase A and B for each stream
        let entry = current_frame.entry(row.stream_name.clone()).or_insert((None, None));
        match row.phase.as_str() {
            "phase_a" => entry.0 = Some(row),
            "phase_b" => entry.1 = Some(row),
            _ => log::warn!("Unknown phase: {}", row.phase),
        }
    }
    
    // Last frame
    if !current_frame.is_empty() {
        frames.push(build_frame(&current_frame, frame_count)?);
    }
    
    log::info!("Loaded {} frames from {}", frames.len(), file_path);
    
    // Setup ZeroMQ publisher
    let mut socket = zeromq::PubSocket::new();
    socket.bind(&pub_addr).await.context("Could not bind to ZeroMQ socket")?;
    
    log::info!("Publisher bound to {}, waiting 75 seconds for subscribers...", pub_addr);
    tokio::time::sleep(Duration::from_secs(75)).await;
    
    log::info!("Publishing {} frames at {} Hz with topic '{}'...", frames.len(), rate_hz, topic);
    
    let period = Duration::from_secs_f64(1.0 / rate_hz);
    let start_time = SystemTime::now();
    
    for (idx, frame) in frames.iter().enumerate() {
        // Rewrite timestamps to NOW + offset for live dashboards
        let offset = period * idx as u32;
        let current_time = start_time + offset;
        let timestamp = current_time.duration_since(UNIX_EPOCH).unwrap();
        
        let mut frame_with_time = frame.clone();
        for calc in frame_with_time.calculations.iter_mut() {
            if let Some(DataProduct::Calculations(ref mut two_phase)) = calc.data_product {
                if let Some(ref mut phase_a) = two_phase.phase_a {
                    if let Some(ref mut prov) = phase_a.provenance {
                        prov.utc_time = Some(prost_types::Timestamp {
                            seconds: timestamp.as_secs() as i64,
                            nanos: timestamp.subsec_nanos() as i32,
                        });
                    }
                }
                if let Some(ref mut phase_b) = two_phase.phase_b {
                    if let Some(ref mut prov) = phase_b.provenance {
                        prov.utc_time = Some(prost_types::Timestamp {
                            seconds: timestamp.as_secs() as i64,
                            nanos: timestamp.subsec_nanos() as i32,
                        });
                    }
                }
            }
        }
        
        // Encode and publish
        let mut buf = Vec::new();
        frame_with_time.encode(&mut buf).context("Failed to encode frame")?;
        
        let mut message = topic.as_bytes().to_vec();
        message.extend_from_slice(&buf);
        
        socket.send(message.into()).await.context("Failed to send message")?;
        tokio::time::sleep(period).await;
    }
    
    log::info!("Finished publishing {} frames.", frames.len());
    
    // Keep container alive
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

fn build_frame(
    frame_data: &HashMap<String, (Option<CsvRow>, Option<CsvRow>)>,
    sequence: u64,
) -> Result<CompositeJoinedCalculations> {
    let mut calculations = Vec::new();
    
    for (stream_name, (phase_a, phase_b)) in frame_data.iter() {
        let Some(row_a) = phase_a else { continue };
        
        // If phase_b is missing from CSV, duplicate phase_a to satisfy protobuf structure.
        // Dashboards only display data fromphase_a. To reduce CSV file size by ~50%, we only
        // export phase_a and duplicate it here. Downstream services (data-exporter and
        // data-db) expect both fields and use .unwrap(), so we populate both.
        let row_b = phase_b.as_ref().unwrap_or(row_a);
        
        let calc_name = format!("threephase/{}", stream_name);
        
        let composite = CompositeTwoPhaseCalculations {
            phase_a: Some(build_composite(row_a, sequence)),
            phase_b: Some(build_composite(row_b, sequence)),
        };
        
        calculations.push(CompositeJoinedCalculationsWrapper {
            calculation_name: Some(calc_name),
            data_product: Some(DataProduct::Calculations(composite)),
        });
    }
    
    Ok(CompositeJoinedCalculations { calculations })
}

fn build_composite(row: &CsvRow, sequence: u64) -> CompositeCalculations {
    CompositeCalculations {
        provenance: Some(Provenance {
            utc_time: Some(prost_types::Timestamp {
                seconds: 0, // Will be overwritten at publish time
                nanos: 0,
            }),
            generic_sequence_number: row.sequence_number.or(Some(sequence)),
        }),
        voltage_waveform_calculations_v: Some(WaveformCalculations {
            rms: Some(row.rms_voltage),
            dc_offset: Some(row.dc_offset_voltage),
        }),
        current_waveform_calculations_a: Some(WaveformCalculations {
            rms: Some(row.rms_current),
            dc_offset: Some(row.dc_offset_current),
        }),
        power_calculations: Some(PowerCalculations {
            real_power_w: Some(row.real_power),
            apparent_power_va: Some(row.apparent_power),
            reactive_power_var: Some(row.reactive_power),
            power_factor: Some(row.power_factor),
        }),
    }
}

