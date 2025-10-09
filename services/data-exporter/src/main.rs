use std::time::Duration;

use axum::{
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use clap::Parser;
use prometheus::{Encoder, TextEncoder};

use crate::data_product_listener::listen;

mod data_product_listener;

#[derive(Clone, Debug, Parser)]
struct Args {
    /// The ip and port of the zmq source
    #[arg(long)]
    pub source: String,
    /// The prometheus port
    #[arg(long)]
    pub prometheus_port: u16,
    // The topic we're subscribing to
    #[arg(long)]
    pub zmq_subscription: String,
}

async fn metrics_handler() -> (StatusCode, HeaderMap, String) {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        log::error!("Failed to encode metrics: {:?}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new(), String::new());
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Type",
        "text/plain; version=0.0.4; charset=utf-8"
            .parse()
            .unwrap(),
    );

    let body = match String::from_utf8(buffer) {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to convert metrics to UTF8: {:?}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new(), String::new());
        }
    };

    log::debug!("Serving {} bytes of metrics", body.len());
    (StatusCode::OK, headers, body)
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    let prom_binding_addr = format!("0.0.0.0:{}", args.prometheus_port);

    // Start metrics server
    let app = Router::new().route("/metrics", get(metrics_handler));
    let listener = tokio::net::TcpListener::bind(&prom_binding_addr)
        .await
        .expect("Could not bind prometheus server");
    log::info!("data-exporter: Prometheus metrics server listening on {}", prom_binding_addr);

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Metrics server failed");
    });

    loop {
        if let Err(err) = listen(args.clone()).await {
            log::error!("Loop exited unexpectedly:{err:#?}, trying again.");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}
