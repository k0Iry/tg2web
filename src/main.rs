use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use azservicebus::core::BasicRetryPolicy;
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

/// 把 client + sender 放在同一个锁里，避免死锁 & 允许 create_sender(&mut self)
struct SbState {
    topic: String,
    inner: Mutex<SbInner>,
}

struct SbInner {
    client: ServiceBusClient<BasicRetryPolicy>,
    sender: Option<ServiceBusSender>,
}

impl SbState {
    async fn ensure_sender_locked(inner: &mut SbInner, topic: &str) -> anyhow::Result<()> {
        if inner.sender.is_none() {
            let s = inner
                .client
                .create_sender(topic.to_string(), ServiceBusSenderOptions::default())
                .await?;
            inner.sender = Some(s);
        }
        Ok(())
    }

    /// 发送：失败则重建 sender 并重试一次（重试时重新构造 message，避免 message 不可复用）
    async fn publish_bytes_with_retry(
        &self,
        body: Vec<u8>,
        message_id: String,
    ) -> anyhow::Result<()> {
        // --- first attempt ---
        {
            let mut inner = self.inner.lock().await;

            Self::ensure_sender_locked(&mut inner, &self.topic).await?;

            let mut msg1 = ServiceBusMessage::new(body.clone());
            msg1.set_message_id(message_id.clone())?;

            let send1 = inner
                .sender
                .as_mut()
                .expect("sender exists after ensure_sender")
                .send_message(msg1)
                .await;

            if send1.is_ok() {
                return Ok(());
            }

            warn!(
                "service bus send failed (likely idle detach), will rebuild sender then retry: {:?}",
                send1.err().unwrap()
            );

            // 标记 sender 失效，释放锁后重建再试
            inner.sender = None;
        }

        // --- rebuild + second attempt ---
        {
            let mut inner = self.inner.lock().await;

            // 重建 sender
            let s = inner
                .client
                .create_sender(self.topic.clone(), ServiceBusSenderOptions::default())
                .await?;
            inner.sender = Some(s);

            // 重试：重新构造 message2
            let mut msg2 = ServiceBusMessage::new(body);
            msg2.set_message_id(message_id)?;

            inner
                .sender
                .as_mut()
                .expect("sender exists after rebuild")
                .send_message(msg2)
                .await?;
        }

        Ok(())
    }
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
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,azservicebus=warn".to_string()),
        )
        .init();

    let secret_token = std::env::var("TELEGRAM_WEBHOOK_SECRET").ok();

    let conn = std::env::var("SERVICEBUS_CONNECTION_STRING")
        .expect("missing SERVICEBUS_CONNECTION_STRING");
    let topic = std::env::var("SERVICEBUS_TOPIC").unwrap_or_else(|_| "posts".to_string());

    let client: ServiceBusClient<BasicRetryPolicy> =
        ServiceBusClient::new_from_connection_string(conn, ServiceBusClientOptions::default())
            .await
            .expect("failed to create ServiceBusClient");

    let sb = Arc::new(SbState {
        topic: topic.clone(),
        inner: Mutex::new(SbInner {
            client,
            sender: None, // lazy create
        }),
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
    if let Some(expected) = state.secret_token.as_deref() {
        let got = headers
            .get("x-telegram-bot-api-secret-token")
            .and_then(|v| v.to_str().ok());
        if got != Some(expected) {
            warn!("webhook rejected: secret token mismatch");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let (msg, edited) = match (update.channel_post, update.edited_channel_post) {
        (Some(m), _) => (m, false),
        (None, Some(m)) => (m, true),
        (None, None) => return (StatusCode::OK, "ignored").into_response(),
    };

    let text = msg.text.or(msg.caption).unwrap_or_default();
    if text.trim().is_empty() {
        return (StatusCode::OK, "empty").into_response();
    }

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

    let body = match serde_json::to_vec(&ev) {
        Ok(b) => b,
        Err(e) => {
            warn!("serialize failed: {:?}", e);
            return (StatusCode::BAD_REQUEST, "bad_payload").into_response();
        }
    };

    if let Err(e) = state
        .sb
        .publish_bytes_with_retry(body, ev.post.id.clone())
        .await
    {
        warn!("failed to publish to service bus: {:?}", e);
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
