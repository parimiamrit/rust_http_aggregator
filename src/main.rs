use std::time::{Duration, Instant};

use actix_web::{get, post, web::Json, App, HttpResponse, HttpServer, Responder};
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
    #[serde(rename = "payload")]
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

#[derive(Deserialize, Serialize)]
struct SampleResponse {
    success: bool,
}
impl SampleResponse {
    pub fn new(success: bool) -> Self {
        SampleResponse { success }
    }
}

async fn process_single_request(
    AggregatorRequest {
        request_id,
        url,
        method,
        payload,
    }: AggregatorRequest,
) -> AggregatorResponse {
    let client = reqwest::Client::new();
    let now = Instant::now();
    match method.as_str() {
        "GET" => {
            let response = client.get(url.clone()).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let bytes = &*unwrapped_response.bytes().await.unwrap();
            let mapping_time = Instant::now();
            let response = serde_json::from_slice::<Value>(bytes).unwrap_or_default();
            log::info!("mapping time: {:?}", mapping_time.elapsed().as_millis());
            return AggregatorResponse::new(
                request_id.clone(),
                url.clone(),
                status,
                response,
                now.elapsed().as_millis(),
            );

            // Probably more efficient to do this?
            /*if unwrapped_response.status() != reqwest::StatusCode::OK {
                return AggregatorResponse::new(
                    request_id.clone(),
                    url.clone(),
                    unwrapped_response.status().as_u16(),
                    serde_json::to_value(unwrapped_response.text().await.unwrap()).unwrap(),
                    now.elapsed().as_millis(),
                );
            }
            return AggregatorResponse::new(
                request_id.clone(),
                url.clone(),
                unwrapped_response.status().as_u16(),
                unwrapped_response.json::<Value>().await.unwrap(),
                now.elapsed().as_millis(),
            );*/
        }
        "POST" => {
            let response = client.post(url.clone()).json(&payload).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let bytes = &*unwrapped_response.bytes().await.unwrap();
            let response = serde_json::from_slice::<Value>(bytes).unwrap_or_default();
            return AggregatorResponse::new(
                request_id.clone(),
                url.clone(),
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        "PATCH" => {
            let response = client.patch(url.clone()).json(&payload).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let bytes = &*unwrapped_response.bytes().await.unwrap();
            let response = serde_json::from_slice::<Value>(bytes).unwrap_or_default();
            return AggregatorResponse::new(
                request_id.clone(),
                url.clone(),
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        _ => {
            return AggregatorResponse::new(
                request_id.clone(),
                url.clone(),
                405,
                serde_json::to_value("Unknown request method").unwrap(),
                now.elapsed().as_millis(),
            );
        }
    }
}

#[utoipa::path(post, responses((status=200, description="Process HTTP requests parallelly", body=Vec<AggregatorResponse>)), request_body(content=Vec<AggregatorRequest>, content_type = "application/json"))]
#[post("/process")]
async fn process(req_body: Json<Vec<AggregatorRequest>>) -> impl Responder {
    println!("request body: {:?}", req_body);

    let req_body_clone = req_body.to_vec();
    let futures: Vec<_> = req_body_clone
        .into_iter()
        .map(|single_req| tokio::spawn(process_single_request(single_req)))
        .collect();

    let mut res = Vec::with_capacity(req_body.len());

    for future in futures.into_iter() {
        res.push(future.await.unwrap());
    }

    HttpResponse::Ok().json(res)
}

#[get("/sample")]
async fn sample_get() -> impl Responder {
    // std::thread::sleep(Duration::from_secs(3));
    HttpResponse::ServiceUnavailable().json(SampleResponse::new(true))
}

#[post("/sample")]
async fn sample_post(req_body: Json<Value>) -> impl Responder {
    // std::thread::sleep(Duration::from_secs(3));
    HttpResponse::Ok().json(req_body)
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
            .wrap(RequestTracing::new())
            .wrap(RequestMetrics::default())
            .service(sample_get)
            .service(sample_post)
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
