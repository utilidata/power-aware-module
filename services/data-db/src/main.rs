use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use prost::Message;
use protobuf_rs::utilidata::karman::bibimbap::v1::{
    CompositeJoinedCalculations, composite_joined_calculations_wrapper::DataProduct,
};
use serde::Serialize;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions, query};
use zeromq::{Socket, SocketRecv, SubSocket};

#[derive(Serialize)]
struct Calculation {
    phase_a: Bucket,
    phase_b: Bucket,
}

#[derive(Serialize)]
struct Bucket {
    rms_current: f64,
    rms_voltage: f64,
    dc_offset_voltage: f64,
    dc_offset_current: f64,
    real_power: f64,
    apparent_power: f64,
    reactive_power: f64,
    power_factor: f64,
    three_phase_real_power: f64,
    three_phase_reactive_power: f64,
}

fn into_json(value: CompositeJoinedCalculations) -> serde_json::Value {
    let mut outside = HashMap::new();

    // let's do some calculations!!!!
    let mut real_power_three_phase_a = 0.0;
    let mut reactive_power_three_phase_a = 0.0;
    let mut real_power_three_phase_b = 0.0;
    let mut reactive_power_three_phase_b = 0.0;
    for j in value.calculations.iter() {
        let DataProduct::Calculations(calc) = j.data_product.as_ref().unwrap() else {
            continue;
        };
        real_power_three_phase_a += calc
            .phase_a
            .unwrap()
            .power_calculations
            .unwrap()
            .real_power_w() as f64;

        reactive_power_three_phase_a += calc
            .phase_a
            .unwrap()
            .power_calculations
            .unwrap()
            .reactive_power_var() as f64;

        real_power_three_phase_b += calc
            .phase_b
            .unwrap()
            .power_calculations
            .unwrap()
            .real_power_w() as f64;

        reactive_power_three_phase_b += calc
            .phase_b
            .unwrap()
            .power_calculations
            .unwrap()
            .reactive_power_var() as f64;
    }

    for joined in value.calculations.iter() {
        let DataProduct::Calculations(calc) = joined.data_product.as_ref().unwrap() else {
            continue;
        };

        let pa = calc.phase_a.unwrap();
        let pb = calc.phase_b.unwrap();

        let calculation = Calculation {
            phase_a: Bucket {
                rms_current: pa.current_waveform_calculations_a.as_ref().unwrap().rms() as f64,
                rms_voltage: pa.voltage_waveform_calculations_v.as_ref().unwrap().rms() as f64,
                dc_offset_voltage: pa
                    .voltage_waveform_calculations_v
                    .as_ref()
                    .unwrap()
                    .dc_offset() as f64,
                dc_offset_current: pa
                    .current_waveform_calculations_a
                    .as_ref()
                    .unwrap()
                    .dc_offset() as f64,
                real_power: pa.power_calculations.as_ref().unwrap().real_power_w() as f64,
                apparent_power: pa.power_calculations.as_ref().unwrap().apparent_power_va() as f64,
                reactive_power: pa.power_calculations.as_ref().unwrap().reactive_power_var() as f64,
                power_factor: pa.power_calculations.as_ref().unwrap().power_factor() as f64,
                three_phase_reactive_power: reactive_power_three_phase_a,
                three_phase_real_power: real_power_three_phase_a,
            },
            phase_b: Bucket {
                rms_current: pb.current_waveform_calculations_a.as_ref().unwrap().rms() as f64,
                rms_voltage: pb.voltage_waveform_calculations_v.as_ref().unwrap().rms() as f64,
                dc_offset_voltage: pb
                    .voltage_waveform_calculations_v
                    .as_ref()
                    .unwrap()
                    .dc_offset() as f64,
                dc_offset_current: pb
                    .current_waveform_calculations_a
                    .as_ref()
                    .unwrap()
                    .dc_offset() as f64,
                real_power: pb.power_calculations.as_ref().unwrap().real_power_w() as f64,
                apparent_power: pb.power_calculations.as_ref().unwrap().apparent_power_va() as f64,
                reactive_power: pb.power_calculations.as_ref().unwrap().reactive_power_var() as f64,
                power_factor: pb.power_calculations.as_ref().unwrap().power_factor() as f64,
                three_phase_reactive_power: reactive_power_three_phase_b,
                three_phase_real_power: real_power_three_phase_b,
            },
        };

        outside.insert(
            joined.calculation_name.as_ref().unwrap().clone(),
            calculation,
        );
    }
    serde_json::to_value(&outside).expect("Could not serialize")
}

async fn prepare_subscribe(endpoint: &str) -> Result<SubSocket> {
    let mut subsocket = SubSocket::new();
    log::info!("about to bind to socket {}", endpoint);
    subsocket
        .connect(endpoint)
        .await
        .context("Could not bind to socket")?;

    log::info!("Bound to socket {}", endpoint);
    Ok(subsocket)
}

async fn listen(args: Args, pool: Pool<Postgres>) {
    let endpoint = match args.resolve_endpoint() {
        Ok(endpoint) => endpoint,
        Err(err) => {
            log::error!("{}", err);
            std::process::exit(2);
        }
    };

    let mut subscription = match prepare_subscribe(&endpoint).await {
        Ok(sock) => sock,
        Err(err) => {
            log::error!("Could not prepare subscription: {err:#?}");
            std::process::exit(255);
        }
    };

    subscription
        .subscribe(&args.zmq_topic.clone())
        .await
        .expect("Could not subscribe");
    loop {
        let incoming = match subscription.recv().await {
            Ok(message) => message,
            Err(err) => {
                log::error!("Unable to receive message: {err:#?}");
                std::process::exit(255);
            }
        };

        let as_vec = incoming.into_vec();

        let Some(frame) = as_vec.first() else {
            log::error!("Weird frameless message");
            continue;
        };

        let buf = &frame[args.zmq_topic.len()..];

        let joined = match CompositeJoinedCalculations::decode(buf) {
            Ok(joined) => joined,
            Err(err) => {
                log::error!("Could not decode incoming message: {err:#?}");
                continue;
            }
        };

        let as_json = into_json(joined);

        if let Err(err) = query("INSERT INTO bibimbap (time, device, data) VALUES ($1, $2, $3)")
            .bind(chrono::Utc::now())
            .bind("bibimbap")
            .bind(as_json)
            .execute(&pool)
            .await
        {
            log::error!("Could not write to table: {err:#?}");
        }
    }
}

#[derive(Parser, Clone)]
struct Args {
    #[arg(long)]
    connection_string: String,
    #[arg(long)]
    zmq_endpoint: Option<String>,
    #[arg(long, default_value = "127.0.0.1")]
    zmq_host: String,
    #[arg(long)]
    zmq_port: Option<u16>,
    #[arg(long)]
    zmq_topic: String,
}

impl Args {
    fn resolve_endpoint(&self) -> Result<String> {
        if let Some(endpoint) = &self.zmq_endpoint {
            return Ok(endpoint.clone());
        }

        let Some(port) = self.zmq_port else {
            return Err(anyhow!(
                "Either --zmq-endpoint or both --zmq-host and --zmq-port must be provided"
            ));
        };

        Ok(format!("tcp://{}:{}", self.zmq_host, port))
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();

    let pool = PgPoolOptions::new()
        .max_connections(5) // tune for your workload
        .connect(&args.connection_string.clone())
        .await
        .expect("Could not connect to database");

    listen(args, pool).await;
}
