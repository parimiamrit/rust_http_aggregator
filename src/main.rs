use std::{collections::HashMap, time::{Duration, Instant}};

use actix_web::{get, post, web::{self, Data, Json}, App, HttpResponse, HttpServer, Responder};
use actix_web_opentelemetry::RequestTracing;
use actix_web_prom::{ActixMetricsConfiguration, PrometheusMetricsBuilder};
use bytes::Buf;
use opentelemetry::{ KeyValue};
use opentelemetry_otlp::{TonicExporterBuilder, WithExportConfig};
use opentelemetry_sdk::{propagation::TraceContextPropagator, runtime::TokioCurrentThread, trace::{self}, Resource};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

const APPLICATION_NAME: &str = "rust-http-aggregator";

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
            let response = serde_json::from_slice::<Value>(unwrapped_response.bytes().await.unwrap().chunk()).unwrap_or_default();
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
            let response = serde_json::from_slice::<Value>(unwrapped_response.bytes().await.unwrap().chunk()).unwrap_or_default();
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
            let response = serde_json::from_slice::<Value>(unwrapped_response.bytes().await.unwrap().chunk()).unwrap_or_default();
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
            let response = serde_json::from_slice::<Value>(unwrapped_response.bytes().await.unwrap().chunk()).unwrap_or_default();
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
    // log::info!("Request body: {:?}", req_body);

    let mut set = tokio::task::JoinSet::new();
    for req in req_body.to_vec() {
        set.spawn(process_single_request(req, Data::clone(&data)));
    }

    let mut result = Vec::with_capacity(req_body.len());

    while let Some(future) = set.join_next().await {
        result.push(future.unwrap());
    }

    HttpResponse::Ok().json(result)
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
    ::std::env::set_var("OTEL_LOG_LEVEL", "INFO");
    env_logger::init();

    let mut labels = HashMap::new();
    labels.insert("application".to_string(), APPLICATION_NAME.to_string());
    let prometheus = PrometheusMetricsBuilder::new("http")
        .endpoint("/metrics")
        .metrics_configuration(ActixMetricsConfiguration::default())
        .const_labels(labels)
        .build()
        .unwrap();

    // Tracing
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
 
    let service_name_resource = Resource::new(vec![KeyValue::new(
        "service.name",
        APPLICATION_NAME
    )]);
 
    // let collector_endpoint = "";
    let collector_endpoint = ::std::env::var("JAEGER_COLLECTOR_URL").unwrap_or("http://localhost:4317".into());
    log::info!("collector endpoint: {:?}", collector_endpoint);
    let _tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(TonicExporterBuilder::default().with_endpoint(collector_endpoint))
    .with_trace_config(
        trace::Config::default()
        .with_resource(service_name_resource)
        // .with_sampler(sampler)
    )
    .install_batch(TokioCurrentThread)
    .expect("pipeline install error");


    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::clone(&web::Data::new(reqwest::Client::new())))
            .wrap(prometheus.clone())
            .wrap(RequestTracing::new())
            .service(health_check)
            .service(process)
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}")
                    .url("/api-docs/openapi.json", ApiDoc::openapi()),
            )
    })
    .disable_signals()
    .bind(("0.0.0.0", 9880))?
    .run()
    .await?;

    Ok(())
}
