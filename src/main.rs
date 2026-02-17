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
use std::{net::SocketAddr, sync::Arc, time::Duration};
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

/// ✅ 连接不稳定时，最硬兜底：失败就丢连接并重建
struct SbState {
    topic: String,
    inner: Mutex<SbInner>,
}

struct SbInner {
    conn: String,
    client: Option<ServiceBusClient<BasicRetryPolicy>>,
    sender: Option<ServiceBusSender>,
}

impl SbState {
    async fn connect_locked(inner: &mut SbInner, topic: &str) -> anyhow::Result<()> {
        let mut client: ServiceBusClient<BasicRetryPolicy> =
            ServiceBusClient::new_from_connection_string(
                inner.conn.clone(),
                ServiceBusClientOptions::default(),
            )
            .await?;

        let sender = client
            .create_sender(topic.to_string(), ServiceBusSenderOptions::default())
            .await?;

        inner.client = Some(client);
        inner.sender = Some(sender);
        Ok(())
    }

    async fn force_reconnect(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        inner.client = None;
        inner.sender = None;
        Self::connect_locked(&mut inner, &self.topic).await
    }

    async fn send_once(&self, body: Vec<u8>, message_id: &str) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;

        if inner.client.is_none() || inner.sender.is_none() {
            Self::connect_locked(&mut inner, &self.topic).await?;
        }

        let mut msg = ServiceBusMessage::new(body);
        msg.set_message_id(message_id.to_string())?;

        inner
            .sender
            .as_mut()
            .expect("sender exists after connect")
            .send_message(msg)
            .await?;

        Ok(())
    }

    /// ✅ 最硬兜底：最多 4 次尝试（3 次退避 + 最后一次）
    pub async fn publish_bytes_with_retry(
        &self,
        body: Vec<u8>,
        message_id: String,
    ) -> anyhow::Result<()> {
        let backoff_ms = [100u64, 500u64, 1000u64];

        for (i, ms) in backoff_ms.iter().enumerate() {
            let attempt_body = body.clone();
            match self.send_once(attempt_body, &message_id).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(
                        "service bus publish attempt {} failed: {:?}; forcing reconnect then retry",
                        i + 1,
                        e
                    );
                    let _ = self.force_reconnect().await;
                    tokio::time::sleep(Duration::from_millis(*ms)).await;
                }
            }
        }

        // 最后一次（不 sleep）
        self.send_once(body, &message_id).await
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
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "info,azservicebus=warn,tower_http=info".to_string()),
        )
        .init();

    let secret_token = std::env::var("TELEGRAM_WEBHOOK_SECRET").ok();

    let conn = std::env::var("SERVICEBUS_CONNECTION_STRING")
        .expect("missing SERVICEBUS_CONNECTION_STRING");
    let topic = std::env::var("SERVICEBUS_TOPIC").unwrap_or_else(|_| "posts".to_string());

    let sb = Arc::new(SbState {
        topic: topic.clone(),
        inner: Mutex::new(SbInner {
            conn,
            client: None,
            sender: None,
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
    // 1) secret token 校验（可选）
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
        (None, None) => return (StatusCode::OK, "ignored").into_response(),
    };

    // 3) text/caption
    let text = msg.text.or(msg.caption).unwrap_or_default();
    if text.trim().is_empty() {
        return (StatusCode::OK, "empty").into_response();
    }

    // 4) tags
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

    // 5) publish to Service Bus（失败重连+重试）
    if let Err(e) = state
        .sb
        .publish_bytes_with_retry(body, ev.post.id.clone())
        .await
    {
        warn!("failed to publish to service bus: {:?}", e);
        // 返回 500 让 Telegram 重试（至少一次投递）
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
