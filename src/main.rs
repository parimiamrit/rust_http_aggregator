use std::{collections::HashMap, str::FromStr, time::Instant};

use actix_web::{get, post, web::{self, Data, Json}, App, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_opentelemetry::RequestTracing;
use actix_web_prometheus::PrometheusMetricsBuilder;
use bytes::Buf;
use opentelemetry::{trace::{FutureExt, TraceContextExt, Tracer}, KeyValue};
use opentelemetry_sdk::{runtime::TokioCurrentThread, trace::{self}, Resource};
use reqwest_middleware::ClientBuilder;
use reqwest_tracing::TracingMiddleware;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

const APPLICATION_NAME: &str = "rust-http-aggregator";


#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
struct AggregatorRequest {
    #[serde(rename = "request")]
    request: Vec<IndividualRequest>,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
struct IndividualRequest {
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
    #[serde(rename = "response")]
    response: Vec<IndividualResponse>,
}

impl AggregatorResponse {
    pub fn new( response: Vec<IndividualResponse> ) -> Self {
        AggregatorResponse {response}
    }
}


#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
struct IndividualResponse {
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

impl IndividualResponse {
    pub fn new(
        request_id: String,
        url: String,
        status: u16,
        response: Value,
        latency: u128,
    ) -> Self {
        IndividualResponse {
            request_id,
            url,
            status,
            response,
            latency,
        }
    }
}


async fn process_single_request(
    IndividualRequest {
        request_id,
        url,
        method,
        payload,
    }: IndividualRequest,
    http_clients: Data<Vec<reqwest_middleware::ClientWithMiddleware>>,
    http_client_index: usize,
) -> IndividualResponse {
    log::info!("Task traceid: {:?}, span: {:?}", opentelemetry::Context::current().span().span_context().trace_id(), opentelemetry::Context::current().span().span_context().span_id());
    let mut headers = reqwest::header::HeaderMap::new();
    headers.append("XATOM-CLIENTID", "rust-http-aggregator".parse().unwrap());
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject(&mut opentelemetry_http::HeaderInjector(&mut headers));
    });

    let now = Instant::now();
    let http_client = http_clients.get(http_client_index).expect("Http Client not found");
    match method.as_str() {
        "GET" => {
            let response = http_client.get(&url).headers(headers).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let response = serde_json::from_slice::<Value>(unwrapped_response.bytes().await.unwrap().chunk()).unwrap_or_default();
            return IndividualResponse::new(
                request_id,
                url,
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        "POST" => {
            let response = http_client.post(&url).headers(headers).body(payload.to_string()).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let response = serde_json::from_slice::<Value>(unwrapped_response.bytes().await.unwrap().chunk()).unwrap_or_default();
            return IndividualResponse::new(
                request_id,
                url,
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        "PUT" => {
            let response = http_client.put(&url).headers(headers).body(payload.to_string()).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let response = serde_json::from_slice::<Value>(unwrapped_response.bytes().await.unwrap().chunk()).unwrap_or_default();
            return IndividualResponse::new(
                request_id,
                url,
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        "PATCH" => {
            let response = http_client.patch(&url).headers(headers).body(payload.to_string()).send().await;
            let unwrapped_response = response.unwrap();

            let status = unwrapped_response.status().as_u16();
            let response = serde_json::from_slice::<Value>(unwrapped_response.bytes().await.unwrap().chunk()).unwrap_or_default();
            return IndividualResponse::new(
                request_id,
                url,
                status,
                response,
                now.elapsed().as_millis(),
            );
        }
        _ => {
            return IndividualResponse::new(
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
async fn process(req_body: Json<AggregatorRequest>, data: web::Data<Vec<reqwest_middleware::ClientWithMiddleware>>, request: HttpRequest) -> impl Responder {

    let tracer = opentelemetry::global::tracer("tracer");
    let span_builder = tracer.span_builder("span_builder");

    let parent_ctx1 = extract_context_from_request(&request).await;
    let parent_span = span_builder.start_with_context(&tracer, &parent_ctx1);
    let _paren_active = opentelemetry::trace::mark_span_as_active(parent_span);

    log::info!("Current traceid: {:?}, span: {:?}", opentelemetry::Context::current().span().span_context().trace_id(), opentelemetry::Context::current().span().span_context().span_id());
    let mut set = tokio::task::JoinSet::new();
    for (index, req) in req_body.request.to_owned().into_iter().enumerate() {
        let child_span = tracer.start(std::format!("child_{:?}", index));
        let _child_active = opentelemetry::trace::mark_span_as_active(child_span);
        set.spawn(process_single_request(req, (&data).clone(), index%50).with_current_context());
    }

    let mut individual_results = Vec::with_capacity(req_body.request.len());

    while let Some(future) = set.join_next().await {
        individual_results.push(future.unwrap());
    };

    let result = AggregatorResponse::new(individual_results);

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

async fn extract_context_from_request(req: &actix_web::HttpRequest) -> opentelemetry::Context {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        let mut headers = reqwest::header::HeaderMap::new();
        let req_headers = req.headers().clone();
        for (header_name, header_value) in req_headers.into_iter() {
            headers.insert(reqwest::header::HeaderName::from_str(header_name.clone().as_str()).expect(""), reqwest::header::HeaderValue::from_str(header_value.to_str().unwrap()).unwrap());
        }
        propagator.extract(&opentelemetry_http::HeaderExtractor(&headers))
    })
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ::std::env::set_var("RUST_LOG", "info");
    ::std::env::set_var("OTEL_LOG_LEVEL", "INFO");
    env_logger::init();

    let mut labels = HashMap::new();
    labels.insert("application".to_string(), APPLICATION_NAME.to_string());
    let prometheus = PrometheusMetricsBuilder::new("")
        .endpoint("/metrics")
        .const_labels(labels)
        .build()
        .unwrap();

    // Tracing
    opentelemetry::global::set_text_map_propagator(opentelemetry_zipkin::Propagator::with_encoding(opentelemetry_zipkin::B3Encoding::MultipleHeader));

    let service_name_resource = Resource::new(vec![KeyValue::new(
        "service.name",
        APPLICATION_NAME
    )]);
 
    // let collector_endpoint = "";
    let collector_endpoint = ::std::env::var("JAEGER_COLLECTOR_URL").unwrap_or("http://localhost:9411/api/v2/spans".into());
    log::info!("collector endpoint: {:?}", collector_endpoint);
    let _tracer = opentelemetry_zipkin::new_pipeline()
    .with_collector_endpoint(collector_endpoint)
    .with_service_name(APPLICATION_NAME)
    .with_http_client(reqwest::Client::new())
    .with_trace_config(
        trace::Config::default()
        .with_resource(service_name_resource)
        // .with_sampler(sampler)
    )
    .install_batch(TokioCurrentThread)
    .expect("pipeline install error");


    let mut clients = Vec::new();
    for _i in 0..50 {
        clients.push(ClientBuilder::new(reqwest::Client::new()).with(TracingMiddleware::default()).build());
    }

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::clone(&web::Data::new(clients.clone())))
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
