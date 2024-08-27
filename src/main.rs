use std::time::{Duration, Instant};

use actix_web::{get, post, web::{self, Data, Json}, App, HttpResponse, HttpServer, Responder};
use actix_web_opentelemetry::{PrometheusMetricsHandler, RequestMetrics, RequestTracing};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    trace::{self, Sampler},
    Resource,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

const APPLICATION_NAME: &str = "rust_http_aggregator";

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
struct AggregatorRequest {
    #[serde(rename = "requestID")]
    request_id: String,
    #[serde(rename = "url")]
    url: String,
    #[serde(rename = "method")]
    method: String,
    #[serde(rename = "payload", default)]
    payload: Value,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
struct AggregatorResponse {
    #[serde(rename = "requestId")]
    request_id: String,
    #[serde(rename = "url")]
    url: String,
    #[serde(rename = "status")]
    status: u16,
    #[serde(rename = "data")]
    response: Value,
    #[serde(rename = "latency")]
    latency: u128,
}

impl AggregatorResponse {
    pub fn new(
        request_id: String,
        url: String,
        status: u16,
        response: Value,
        latency: u128,
    ) -> Self {
        AggregatorResponse {
            request_id,
            url,
            status,
            response,
            latency,
        }
    }
}


async fn process_single_request(
    AggregatorRequest {
        request_id,
        url,
        method,
        payload,
    }: AggregatorRequest,
    http_client: Data<reqwest::Client>,
) -> AggregatorResponse {
    let now = Instant::now();
    match method.as_str() {
        "GET" => {
            let response = http_client.get(&url).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let bytes = &*unwrapped_response.bytes().await.unwrap();
            let response = serde_json::from_slice::<Value>(bytes).unwrap_or_default();
            return AggregatorResponse::new(
                request_id,
                url,
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        "POST" => {
            let response = http_client.post(&url).json(&payload).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let bytes = &*unwrapped_response.bytes().await.unwrap();
            let response = serde_json::from_slice::<Value>(bytes).unwrap_or_default();
            return AggregatorResponse::new(
                request_id,
                url,
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        "PUT" => {
            let response = http_client.put(&url).json(&payload).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let bytes = &*unwrapped_response.bytes().await.unwrap();
            let response = serde_json::from_slice::<Value>(bytes).unwrap_or_default();
            return AggregatorResponse::new(
                request_id,
                url,
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        "PATCH" => {
            let response = http_client.patch(&url).json(&payload).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let bytes = &*unwrapped_response.bytes().await.unwrap();
            let response = serde_json::from_slice::<Value>(bytes).unwrap_or_default();
            return AggregatorResponse::new(
                request_id,
                url,
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        _ => {
            return AggregatorResponse::new(
                request_id,
                url,
                405,
                serde_json::to_value("Unknown request method").unwrap(),
                now.elapsed().as_millis(),
            );
        }
    }
}

#[utoipa::path(post, responses((status=200, description="Process HTTP requests parallelly", body=Vec<AggregatorResponse>)), request_body(content=Vec<AggregatorRequest>, content_type = "application/json"))]
#[post("/process")]
async fn process(req_body: Json<Vec<AggregatorRequest>>, data: web::Data<reqwest::Client>) -> impl Responder {
    println!("request body: {:?}", req_body);

    let req_body_clone = req_body.to_vec();
    let futures: Vec<_> = req_body_clone
        .into_iter()
        .map(|single_req| tokio::spawn(process_single_request(single_req, Data::clone(&data))))
        .collect();

    let mut res = Vec::with_capacity(req_body.len());

    for future in futures.into_iter() {
        res.push(future.await.unwrap());
    }

    HttpResponse::Ok().json(res)
}

#[get("/health")]
async fn health_check() -> impl Responder {
    HttpResponse::Ok()
}

#[derive(OpenApi)]
#[openapi(
    paths(process),
    components(schemas(AggregatorRequest, AggregatorResponse))
)]
struct ApiDoc;

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ::std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    // Configure prometheus or your preferred metrics service
    let registry = prometheus::Registry::new();
    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .unwrap();
    // set up your meter provider with your exporter(s)
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(prometheus_exporter)
        .build();
    global::set_meter_provider(meter_provider.clone());

    global::set_text_map_propagator(opentelemetry_jaeger_propagator::Propagator::new());
    let jaeger_tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(
                    std::env::var("JAEGER_COLLECTOR_URL")
                        .unwrap_or("http://localhost:4317".to_string()),
                )
                .with_timeout(Duration::from_secs(3)),
        )
        .with_trace_config(
            trace::Config::default()
                .with_sampler(Sampler::AlwaysOn)
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    APPLICATION_NAME,
                )])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("Failed to install OpenTelemetry tracer.");
    global::set_tracer_provider(jaeger_tracer_provider);
    log::info!("Tracer setup done!");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::clone(&web::Data::new(reqwest::Client::new())))
            .wrap(RequestTracing::new())
            .wrap(RequestMetrics::default())
            .service(health_check)
            .service(process)
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}")
                    .url("/api-docs/openapi.json", ApiDoc::openapi()),
            )
            .route(
                "/metrics",
                actix_web::web::get().to(PrometheusMetricsHandler::new(registry.clone())),
            )
    })
    .disable_signals()
    .bind(("127.0.0.1", 8080))?
    .run()
    .await?;

    global::shutdown_tracer_provider();

    meter_provider.shutdown()?;

    Ok(())
}
