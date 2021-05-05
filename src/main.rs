mod config;
mod http;
mod kafka;
mod log;
mod metrics;

use crate::log::kflog;
use clap::ArgMatches;
use kflog::Logger;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use warp::Filter;

fn app_args<'a>() -> ArgMatches<'a> {
    return clap::App::new("kprf")
        .version(clap::crate_version!())
        .about("Kafka Producer Proxy")
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .help("Config file path")
                .takes_value(true),
        )
        .get_matches();
}

fn start_metrics_server(logger: Logger, shutdown_rx: Receiver<String>) -> Receiver<i8> {
    let route = warp::path!("metrics")
        .and(metrics::metrics::with_logger(logger.clone()))
        .and_then(metrics::metrics::handler);

    let (shutdown_completed_tx, shutdown_completed_rx) = oneshot::channel::<i8>();

    let (_, server) = warp::serve(route).bind_with_graceful_shutdown(([0, 0, 0, 0], 8088), {
        async move {
            shutdown_rx.await.ok();
            slog::info!(logger, "shutting down metrics server");
            let send_result = shutdown_completed_tx.send(0);
            if send_result.is_err() {
                slog::error!(
                    logger,
                    "failed to send data to main_server shutdown channel: {}",
                    send_result.err().unwrap()
                );
            }
        }
    });
    let result = tokio::task::spawn(server);
    tokio::task::spawn(async move { result.await });
    return shutdown_completed_rx;
}

#[tokio::main]
async fn main() {
    let args = app_args();
    let mut cfg = config::Config::new(args);
    cfg.prepare();

    let http_config = cfg.get_http_config();
    let logger = kflog::new_logger(&cfg.get_output_file());

    let http_server_config = http::server::Config::new(http_config.port());
    let mut server = http::server::Server::new_from_config(http_server_config);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<String>();
    let (shutdown_metrics_tx, shutdown_metrics_rx) = oneshot::channel::<String>();

    let kafka_producer = kafka::kafka::producer::new(cfg.get_kafka_config());

    let metrics_shutdown_rx = start_metrics_server(logger.clone(), shutdown_metrics_rx);

    // TODO(shmel1k): improve graceful shutdown behavior.
    let main_server_shutdown_rx =
        server.start_server(logger.clone(), kafka_producer.clone(), shutdown_rx);
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            slog::info!(logger, "shutting down application");
            shutdown_tx.send(String::from("shutdown")).expect("failed to shutdown kafka-http server");
            shutdown_metrics_tx.send(String::from("shutdown")).expect("failed to shutdown metrics-http server");
            metrics_shutdown_rx.await.ok();
            main_server_shutdown_rx.await.ok();
        }
    }
}