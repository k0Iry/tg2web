use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use azservicebus::core::BasicRetryPolicy; // ✅ 修复 ServiceBusClient 的泛型
use azservicebus::prelude::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    secret_token: Option<String>,
    sb: Arc<SbState>,
}

struct SbState {
    // ✅ ServiceBusClient 是泛型：ServiceBusClient<RP>
    _client: ServiceBusClient<BasicRetryPolicy>,
    sender: Mutex<ServiceBusSender>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Post {
    id: String, // tg:<chat_id>:<message_id>
    chat_id: i64,
    message_id: i64,
    date: i64, // unix seconds
    text: String,
    tags: Vec<String>,
    edited: bool,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostEvent {
    kind: String, // "new_post" | "edited_post"
    post: Post,
}

#[derive(Debug, Deserialize)]
struct TgUpdate {
    update_id: i64,
    #[serde(default)]
    channel_post: Option<TgMessage>,
    #[serde(default)]
    edited_channel_post: Option<TgMessage>,
}

#[derive(Debug, Deserialize)]
struct TgMessage {
    message_id: i64,
    date: i64,
    chat: TgChat,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    caption: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TgChat {
    id: i64,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    username: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let secret_token = std::env::var("TELEGRAM_WEBHOOK_SECRET").ok();

    let conn = std::env::var("SERVICEBUS_CONNECTION_STRING")
        .expect("missing SERVICEBUS_CONNECTION_STRING");
    let topic = std::env::var("SERVICEBUS_TOPIC").unwrap_or_else(|_| "posts".to_string());

    // ✅ 创建并持有 client + sender（一次性）
    let mut client: ServiceBusClient<BasicRetryPolicy> =
        ServiceBusClient::new_from_connection_string(conn, ServiceBusClientOptions::default())
            .await
            .expect("failed to create ServiceBusClient");

    let sender = client
        .create_sender(topic, ServiceBusSenderOptions::default())
        .await
        .expect("failed to create ServiceBusSender");

    let sb = Arc::new(SbState {
        _client: client,
        sender: Mutex::new(sender),
    });

    let state = AppState { secret_token, sb };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/telegram/webhook", post(telegram_webhook))
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 80));
    info!("webhook listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn telegram_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(update): Json<TgUpdate>,
) -> impl IntoResponse {
    // 1) 校验 secret token（建议开启）
    if let Some(expected) = state.secret_token.as_deref() {
        let got = headers
            .get("x-telegram-bot-api-secret-token")
            .and_then(|v| v.to_str().ok());
        if got != Some(expected) {
            warn!("webhook rejected: secret token mismatch");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    // 2) 取 message：优先 channel_post，其次 edited_channel_post
    let (msg, edited) = match (update.channel_post, update.edited_channel_post) {
        (Some(m), _) => (m, false),
        (None, Some(m)) => (m, true),
        (None, None) => {
            // 不是频道消息，直接 200 避免 Telegram 重试
            return (StatusCode::OK, "ignored").into_response();
        }
    };

    // 3) 提取文本（text 或 caption）
    let text = msg.text.or(msg.caption).unwrap_or_default();
    if text.trim().is_empty() {
        return (StatusCode::OK, "empty").into_response();
    }

    // 4) 解析 tags：#tag
    let tags = extract_tags(&text);

    let post = Post {
        id: format!("tg:{}:{}", msg.chat.id, msg.message_id),
        chat_id: msg.chat.id,
        message_id: msg.message_id,
        date: msg.date,
        text,
        tags,
        edited,
        received_at: Utc::now(),
    };

    let kind = if edited { "edited_post" } else { "new_post" }.to_string();
    let ev = PostEvent { kind, post };

    // 5) publish 到 Service Bus
    let body = match serde_json::to_vec(&ev) {
        Ok(b) => b,
        Err(e) => {
            warn!("serialize failed: {:?}", e);
            return (StatusCode::BAD_REQUEST, "bad_payload").into_response();
        }
    };

    let mut sb_msg = ServiceBusMessage::new(body);
    if let Err(e) = sb_msg.set_message_id(ev.post.id.clone()) {
        warn!("failed to set message id: {:?}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "set_message_id failed").into_response();
    } // ✅ 正确设置 message_id

    let mut sender = state.sb.sender.lock().await;
    if let Err(e) = sender.send_message(sb_msg).await {
        warn!("failed to publish to service bus: {:?}", e);
        // 返回 500 让 Telegram 重试（更接近至少一次）
        return (StatusCode::INTERNAL_SERVER_ERROR, "publish_failed").into_response();
    }

    (StatusCode::OK, "ok").into_response()
}

fn extract_tags(text: &str) -> Vec<String> {
    let mut tags = Vec::new();
    for token in text.split_whitespace() {
        if let Some(stripped) = token.strip_prefix('#') {
            let t = stripped
                .trim_matches(|c: char| !c.is_alphanumeric() && c != '_')
                .to_string();
            if !t.is_empty() {
                tags.push(t);
            }
        }
    }
    tags.sort();
    tags.dedup();
    tags
}
