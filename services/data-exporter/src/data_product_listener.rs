use std::{
    collections::{HashMap, VecDeque},
    sync::LazyLock,
};

use anyhow::{Context, Result};
use prost::Message;
use protobuf_rs::utilidata::karman::bibimbap::v1::{
    composite_joined_calculations_wrapper::DataProduct, CompositeCalculations,
    CompositeJoinedCalculations, CompositeTwoPhaseCalculations,
};
use zeromq::{Socket, SocketRecv, SubSocket};

use crate::Args;

// Ideally you'd use a macro for this kind of thing tbh

macro_rules! build_gauge {
    ($variable_name:ident, $name:expr, $description:expr) => {
        static $variable_name: LazyLock<prometheus::GaugeVec> = LazyLock::new(|| {
            prometheus::register_gauge_vec!($name, $description, &["stream", "phase"],)
                .expect("Unable to register gauge vec")
        });
    };
}

// active power
build_gauge!(
    ACTIVE_POWER_LATEST_GAUGE,
    "active_power_latest",
    "Most recent watts"
);

build_gauge!(ACTIVE_POWER_PEAK_GAUGE, "active_power_peak", "peak watts");
build_gauge!(
    ACTIVE_POWER_TROUGH_GAUGE,
    "active_power_trough",
    "trough active power"
);
build_gauge!(
    ACTIVE_POWER_AVERAGE_GAUGE,
    "active_power_average",
    "average active power"
);

// power factor
build_gauge!(
    POWER_FACTOR_LATEST_GAUGE,
    "power_factor_latest",
    "Most recent"
);

build_gauge!(POWER_FACTOR_PEAK_GAUGE, "power_factor_peak", "peak");
build_gauge!(
    POWER_FACTOR_TROUGH_GAUGE,
    "power_factor_trough",
    "trough power factor"
);
build_gauge!(
    POWER_FACTOR_AVERAGE_GAUGE,
    "power_factor_average",
    "average power factor"
);

// dc offset current
build_gauge!(
    DC_OFFSET_CURRENT_LATEST_GAUGE,
    "dc_offset_current_latest",
    "Most recent"
);

build_gauge!(
    DC_OFFSET_CURRENT_PEAK_GAUGE,
    "dc_offset_current_peak",
    "peak"
);
build_gauge!(
    DC_OFFSET_CURRENT_TROUGH_GAUGE,
    "dc_offset_current_trough",
    "trough"
);
build_gauge!(
    DC_OFFSET_CURRENT_AVERAGE_GAUGE,
    "dc_offset_current_average",
    "average"
);

// dc offset voltage
build_gauge!(
    DC_OFFSET_VOLTAGE_LATEST_GAUGE,
    "dc_offset_voltage_latest",
    "Most recent"
);

build_gauge!(
    DC_OFFSET_VOLTAGE_PEAK_GAUGE,
    "dc_offset_voltage_peak",
    "peak"
);
build_gauge!(
    DC_OFFSET_VOLTAGE_TROUGH_GAUGE,
    "dc_offset_voltage_trough",
    "trough"
);
build_gauge!(
    DC_OFFSET_VOLTAGE_AVERAGE_GAUGE,
    "dc_offset_voltage_average",
    "average"
);

// reactive power
build_gauge!(
    REACTIVE_POWER_LATEST_GAUGE,
    "reactive_power_latest",
    "Most recent watts"
);

build_gauge!(
    REACTIVE_POWER_PEAK_GAUGE,
    "reactive_power_peak",
    "peak watts"
);
build_gauge!(
    REACTIVE_POWER_TROUGH_GAUGE,
    "reactive_power_trough",
    "trough active power"
);
build_gauge!(
    REACTIVE_POWER_AVERAGE_GAUGE,
    "reactive_power_average",
    "average active power"
);

// rms current
build_gauge!(
    RMS_CURRENT_LATEST_GAUGE,
    "rms_current_latest",
    "Most recent"
);

build_gauge!(RMS_CURRENT_PEAK_GAUGE, "rms_current_peak", "peak");
build_gauge!(RMS_CURRENT_TROUGH_GAUGE, "rms_current_trough", "trough");
build_gauge!(RMS_CURRENT_AVERAGE_GAUGE, "rms_current_average", "average");

// rms voltage
build_gauge!(
    RMS_VOLTAGE_LATEST_GAUGE,
    "rms_voltage_latest",
    "Most recent"
);

build_gauge!(RMS_VOLTAGE_PEAK_GAUGE, "rms_voltage_peak", "peak");
build_gauge!(RMS_VOLTAGE_TROUGH_GAUGE, "rms_voltage_trough", "trough");
build_gauge!(RMS_VOLTAGE_AVERAGE_GAUGE, "rms_voltage_average", "average");

// real_power
build_gauge!(REAL_POWER_LATEST_GAUGE, "real_power_latest", "Most recent");

build_gauge!(REAL_POWER_PEAK_GAUGE, "real_power_peak", "peak");
build_gauge!(REAL_POWER_TROUGH_GAUGE, "real_power_trough", "trough");
build_gauge!(REAL_POWER_AVERAGE_GAUGE, "real_power_average", "average");

//    real_power: Bucket,
build_gauge!(
    APPARENT_POWER_LATEST_GAUGE,
    "apparent_power_latest",
    "Most recent"
);

build_gauge!(APPARENT_POWER_PEAK_GAUGE, "apparent_power_peak", "peak");
build_gauge!(
    APPARENT_POWER_TROUGH_GAUGE,
    "apparent_power_trough",
    "trough"
);
build_gauge!(
    APPARENT_POWER_AVERAGE_GAUGE,
    "apparent_power_average",
    "average"
);

build_gauge!(
    REAL_POWER_THREE_PHASE_LATEST_GAUGE,
    "real_power_three_phase_latest",
    "real power three phase peak"
);
build_gauge!(
    REAL_POWER_THREE_PHASE_TROUGH_GAUGE,
    "real_power_three_phase_trough",
    "real power three phase trough"
);
build_gauge!(
    REAL_POWER_THREE_PHASE_AVERAGE_GAUGE,
    "real_power_three_phase_average",
    "real power three phase average"
);
build_gauge!(
    REAL_POWER_THREE_PHASE_PEAK_GAUGE,
    "real_power_three_phase_peak",
    "real power three phase peak"
);

build_gauge!(
    REACTIVE_POWER_THREE_PHASE_LATEST_GAUGE,
    "reactive_power_three_phase_latest",
    "reactive power three phase peak"
);
build_gauge!(
    REACTIVE_POWER_THREE_PHASE_TROUGH_GAUGE,
    "reactive_power_three_phase_trough",
    "reactive power three phase trough"
);
build_gauge!(
    REACTIVE_POWER_THREE_PHASE_AVERAGE_GAUGE,
    "reactive_power_three_phase_average",
    "reactive power three phase average"
);
build_gauge!(
    REACTIVE_POWER_THREE_PHASE_PEAK_GAUGE,
    "reactive_power_three_phase_peak",
    "reactive power three phase peak"
);

pub async fn prepare_subscribe(config: Args) -> Result<SubSocket> {
    let endpoint = format!("tcp://{}", config.source);
    let mut subsocket = SubSocket::new();
    log::info!("about to bind to socket {}", endpoint);
    subsocket.connect(&endpoint).await?;
    log::info!("connected to remote {}", endpoint);
    Ok(subsocket)
}

pub async fn listen(config: Args) -> Result<()> {
    let mut subscription = prepare_subscribe(config.clone()).await?;

    let mut measurements = AllMeasurements::new();
    let mut three_phase = AllThreePhase::default();

    subscription
        .subscribe(&config.zmq_subscription.clone())
        .await
        .context("Could not subscribe")?;
    log::info!("Subscribed to topic: '{}'", config.zmq_subscription);

    // ZeroMQ "slow joiner" workaround: give subscription time to propagate
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    log::info!("Subscription ready, waiting for messages...");

    let mut msg_count = 0;
    loop {
        let incoming = subscription.recv().await?;
        msg_count += 1;
        if msg_count % 100 == 0 {
            log::info!("Received {} messages so far", msg_count);
        }

        let as_vec = incoming.into_vec();

        let Some(frame) = as_vec.first() else {
            log::error!("Weird frameless message");
            continue;
        };

        let buf = &frame[config.zmq_subscription.len()..];

        let joined = match CompositeJoinedCalculations::decode(buf) {
            Ok(joined) => joined,
            Err(err) => {
                log::error!("Could not decode incoming message: {err:#?}");
                continue;
            }
        };

        let mut three_phase_active_a = 0.0;
        let mut three_phase_reactive_a = 0.0;
        let mut three_phase_active_b = 0.0;
        let mut three_phase_reactive_b = 0.0;

        for composite in joined.calculations.into_iter() {
            measurements.apply(
                composite.calculation_name.clone().unwrap().as_str(),
                composite.data_product.clone().unwrap(),
            );
            measurements.update(&composite.calculation_name());

            let Some(DataProduct::Calculations(calcs)) = composite.data_product else {
                continue;
            };
            three_phase_active_a += calcs
                .phase_a
                .unwrap()
                .power_calculations
                .unwrap()
                .real_power_w();
            three_phase_reactive_a += calcs
                .phase_a
                .unwrap()
                .power_calculations
                .unwrap()
                .reactive_power_var();
            three_phase_active_b += calcs
                .phase_b
                .unwrap()
                .power_calculations
                .unwrap()
                .real_power_w();
            three_phase_reactive_b += calcs
                .phase_b
                .unwrap()
                .power_calculations
                .unwrap()
                .reactive_power_var();
        }

        // Okay, this is a little hacky
        three_phase.apply_and_update(
            config.zmq_subscription.clone(),
            three_phase_active_a,
            three_phase_reactive_a,
            three_phase_active_b,
            three_phase_reactive_b,
        );
    }
}

#[derive(Default)]
struct AllThreePhase {
    map: HashMap<String, ThreePhaseMeasurements>,
}

impl AllThreePhase {
    fn apply_and_update(
        &mut self,
        name: String,
        real_a: f32,
        reactive_a: f32,
        real_b: f32,
        reactive_b: f32,
    ) {
        let measurements = self
            .map
            .entry(name.to_string())
            .or_insert(ThreePhaseMeasurements::default());

        measurements.apply(real_a, reactive_a, real_b, reactive_b);

        measurements.update(&name);
    }
}

#[derive(Default)]
struct ThreePhaseMeasurements {
    three_phase_real_a: Bucket,
    three_phase_real_b: Bucket,
    three_phase_reactive_a: Bucket,
    three_phase_reactive_b: Bucket,
}

impl ThreePhaseMeasurements {
    fn apply(&mut self, real_a: f32, reactive_a: f32, real_b: f32, reactive_b: f32) {
        self.three_phase_real_a.apply(real_a as f64);
        self.three_phase_real_b.apply(real_b as f64);
        self.three_phase_reactive_a.apply(reactive_a as f64);
        self.three_phase_reactive_b.apply(reactive_b as f64);
    }

    fn update(&mut self, label: &str) {
        REAL_POWER_THREE_PHASE_AVERAGE_GAUGE
            .with_label_values(&[label, "a"])
            .set(self.three_phase_real_a.average());
        REAL_POWER_THREE_PHASE_LATEST_GAUGE
            .with_label_values(&[label, "a"])
            .set(self.three_phase_real_a.latest());
        REAL_POWER_THREE_PHASE_TROUGH_GAUGE
            .with_label_values(&[label, "a"])
            .set(self.three_phase_real_a.trough());
        REAL_POWER_THREE_PHASE_PEAK_GAUGE
            .with_label_values(&[label, "a"])
            .set(self.three_phase_real_a.peak());

        REAL_POWER_THREE_PHASE_AVERAGE_GAUGE
            .with_label_values(&[label, "b"])
            .set(self.three_phase_real_b.average());
        REAL_POWER_THREE_PHASE_LATEST_GAUGE
            .with_label_values(&[label, "b"])
            .set(self.three_phase_real_b.latest());
        REAL_POWER_THREE_PHASE_TROUGH_GAUGE
            .with_label_values(&[label, "b"])
            .set(self.three_phase_real_b.trough());
        REAL_POWER_THREE_PHASE_PEAK_GAUGE
            .with_label_values(&[label, "b"])
            .set(self.three_phase_real_b.peak());

        REACTIVE_POWER_THREE_PHASE_AVERAGE_GAUGE
            .with_label_values(&[label, "a"])
            .set(self.three_phase_reactive_a.average());
        REACTIVE_POWER_THREE_PHASE_LATEST_GAUGE
            .with_label_values(&[label, "a"])
            .set(self.three_phase_reactive_a.latest());
        REACTIVE_POWER_THREE_PHASE_TROUGH_GAUGE
            .with_label_values(&[label, "a"])
            .set(self.three_phase_reactive_a.trough());
        REACTIVE_POWER_THREE_PHASE_PEAK_GAUGE
            .with_label_values(&[label, "a"])
            .set(self.three_phase_reactive_a.peak());

        REACTIVE_POWER_THREE_PHASE_AVERAGE_GAUGE
            .with_label_values(&[label, "b"])
            .set(self.three_phase_reactive_b.average());
        REACTIVE_POWER_THREE_PHASE_LATEST_GAUGE
            .with_label_values(&[label, "b"])
            .set(self.three_phase_reactive_b.latest());
        REACTIVE_POWER_THREE_PHASE_TROUGH_GAUGE
            .with_label_values(&[label, "b"])
            .set(self.three_phase_reactive_b.trough());
        REACTIVE_POWER_THREE_PHASE_PEAK_GAUGE
            .with_label_values(&[label, "b"])
            .set(self.three_phase_reactive_b.peak())
    }
}

// This structure mirrors the protobuf
// There's probably a better way of doing this.
struct AllMeasurements {
    data: HashMap<String, ConjoinedMeasurements>,
}

impl AllMeasurements {
    fn new() -> Self {
        Self {
            data: HashMap::default(),
        }
    }

    fn apply(&mut self, name: &str, product: DataProduct) {
        let DataProduct::Calculations(calcs) = product else {
            return;
        };

        let measurements = self
            .data
            .entry(name.to_string())
            .or_insert(ConjoinedMeasurements::default());

        measurements.apply(calcs);
    }

    fn update(&mut self, name: &str) {
        let Some(measurements) = self.data.get(name) else {
            self.data
                .insert(name.to_string(), ConjoinedMeasurements::default());
            return;
        };

        measurements.update(name);
    }
}

#[derive(Default)]
struct ConjoinedMeasurements {
    phase_a: MeasurementBuckets,
    phase_b: MeasurementBuckets,
}

impl ConjoinedMeasurements {
    fn apply(&mut self, calcs: CompositeTwoPhaseCalculations) {
        self.phase_a.apply(calcs.phase_a.unwrap());
        self.phase_b.apply(calcs.phase_b.unwrap());
    }

    fn update(&self, name: &str) {
        self.phase_a.update(name, "a");
        self.phase_b.update(name, "b");
    }
}

#[derive(Default)]
struct MeasurementBuckets {
    real_power: Bucket,
    rms_current: Bucket,
    rms_voltage: Bucket,
    apparent_power: Bucket,
    active_power: Bucket,
    reactive_power: Bucket,
    power_factor: Bucket,
    dc_offset_current: Bucket,
    dc_offset_voltage: Bucket,
}

impl MeasurementBuckets {
    fn apply(&mut self, calcs: CompositeCalculations) {
        self.dc_offset_current
            .apply(calcs.current_waveform_calculations_a.unwrap().dc_offset() as f64);
        self.rms_current
            .apply(calcs.current_waveform_calculations_a.unwrap().rms() as f64);
        self.dc_offset_voltage
            .apply(calcs.voltage_waveform_calculations_v.unwrap().dc_offset() as f64);
        self.rms_voltage
            .apply(calcs.voltage_waveform_calculations_v.unwrap().rms() as f64);

        self.apparent_power
            .apply(calcs.power_calculations.unwrap().apparent_power_va() as f64);

        self.power_factor
            .apply(calcs.power_calculations.unwrap().power_factor() as f64);

        self.reactive_power
            .apply(calcs.power_calculations.unwrap().reactive_power_var() as f64);

        self.real_power
            .apply(calcs.power_calculations.unwrap().real_power_w() as f64);
        self.active_power
            .apply(calcs.power_calculations.unwrap().real_power_w() as f64);
    }

    fn update(&self, stream: &str, phase: &str) {
        ACTIVE_POWER_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.active_power.average());
        ACTIVE_POWER_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.active_power.latest());
        ACTIVE_POWER_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.active_power.peak());
        ACTIVE_POWER_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.active_power.trough());

        REAL_POWER_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.real_power.average());
        REAL_POWER_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.real_power.latest());
        REAL_POWER_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.real_power.peak());
        REAL_POWER_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.real_power.trough());

        RMS_CURRENT_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.rms_current.average());
        RMS_CURRENT_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.rms_current.latest());
        RMS_CURRENT_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.rms_current.peak());
        RMS_CURRENT_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.rms_current.trough());

        RMS_VOLTAGE_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.rms_voltage.average());
        RMS_VOLTAGE_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.rms_voltage.latest());
        RMS_VOLTAGE_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.rms_voltage.peak());
        RMS_VOLTAGE_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.rms_voltage.trough());

        APPARENT_POWER_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.apparent_power.average());
        APPARENT_POWER_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.apparent_power.latest());
        APPARENT_POWER_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.apparent_power.peak());
        APPARENT_POWER_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.apparent_power.trough());

        REACTIVE_POWER_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.reactive_power.average());
        REACTIVE_POWER_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.reactive_power.latest());
        REACTIVE_POWER_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.reactive_power.peak());
        REACTIVE_POWER_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.reactive_power.trough());

        POWER_FACTOR_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.power_factor.average());
        POWER_FACTOR_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.power_factor.latest());
        POWER_FACTOR_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.power_factor.peak());
        POWER_FACTOR_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.power_factor.trough());

        DC_OFFSET_CURRENT_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.dc_offset_current.average());
        DC_OFFSET_CURRENT_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.dc_offset_current.latest());
        DC_OFFSET_CURRENT_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.dc_offset_current.peak());
        DC_OFFSET_CURRENT_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.dc_offset_current.trough());

        DC_OFFSET_VOLTAGE_AVERAGE_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.dc_offset_voltage.average());
        DC_OFFSET_VOLTAGE_LATEST_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.dc_offset_voltage.latest());
        DC_OFFSET_VOLTAGE_PEAK_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.dc_offset_voltage.peak());
        DC_OFFSET_VOLTAGE_TROUGH_GAUGE
            .with_label_values(&[stream, phase])
            .set(self.dc_offset_voltage.trough());
    }
}

pub struct Bucket {
    values: VecDeque<f64>,
}

impl Default for Bucket {
    fn default() -> Self {
        Bucket {
            // assuming 60hz samples
            // TODO: learn this.
            values: VecDeque::with_capacity(60 * 5),
        }
    }
}

impl Bucket {
    fn apply(&mut self, val: f64) {
        if self.values.len() == self.values.capacity() {
            self.values.pop_front();
        }

        self.values.push_back(val);
    }

    fn peak(&self) -> f64 {
        self.values
            .iter()
            .fold(f64::MIN, |acc, v| if *v > acc { *v } else { acc })
    }
    fn trough(&self) -> f64 {
        self.values
            .iter()
            .fold(f64::MAX, |acc, v| if *v < acc { *v } else { acc })
    }
    fn average(&self) -> f64 {
        self.values.iter().map(|v| *v).sum::<f64>() / self.values.len() as f64
    }
    fn latest(&self) -> f64 {
        self.values.iter().map(|v| *v).last().unwrap_or_default()
    }
}
