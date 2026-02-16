use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Sse},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use futures::Stream; // 只用它的 Stream trait
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::{
    wrappers::{BroadcastStream, IntervalStream},
    StreamExt, // 这一行非常关键：merge 在这里
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    secret_token: Option<String>,
    // 内存存储：最新 N 条（MVP）
    feed: Arc<RwLock<VecDeque<Post>>>,
    // 实时广播：SSE 使用
    tx: broadcast::Sender<PostEvent>,
    max_items: usize,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
struct PostEvent {
    kind: &'static str, // "new_post" | "edited_post"
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
    // 如果你后面要支持 caption / media，再扩
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
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,tower_http=info".to_string()),
        )
        .init();

    let (tx, _rx) = broadcast::channel::<PostEvent>(1024);

    let state = AppState {
        secret_token: std::env::var("TELEGRAM_WEBHOOK_SECRET").ok(),
        feed: Arc::new(RwLock::new(VecDeque::new())),
        tx,
        max_items: std::env::var("MAX_ITEMS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500),
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/telegram/webhook", post(telegram_webhook))
        .route("/api/feed", get(get_feed))
        .route("/api/stream", get(sse_stream))
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 80));
    info!("listening on http://{}", addr);

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
    // 过滤空消息（你也可以保留）
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

    // 5) 写入内存 feed（MVP），并广播事件（SSE 用）
    {
        let mut feed = state.feed.write().await;

        // 幂等更新：如果已有同 id，则替换
        if let Some(idx) = feed.iter().position(|p| p.id == post.id) {
            feed[idx] = post.clone();
        } else {
            feed.push_front(post.clone());
            if feed.len() > state.max_items {
                feed.pop_back();
            }
        }
    }

    let kind = if edited { "edited_post" } else { "new_post" };
    let _ = state.tx.send(PostEvent { kind, post });

    // 返回 200，Telegram 就不会重试
    (StatusCode::OK, "ok").into_response()
}

async fn get_feed(State(state): State<AppState>) -> impl IntoResponse {
    let feed = state.feed.read().await;
    let items: Vec<Post> = feed.iter().cloned().collect();
    Json(serde_json::json!({
        "items": items,
        "count": items.len()
    }))
}

async fn sse_stream(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>> {
    // 每个连接拿到一个 receiver
    let rx = state.tx.subscribe();

    // 心跳：避免代理/浏览器断开
    let heartbeat = IntervalStream::new(tokio::time::interval(Duration::from_secs(15))).map(|_| {
        Ok(axum::response::sse::Event::default()
            .event("ping")
            .data("1"))
    });

    let events = BroadcastStream::new(rx)
    .map(|msg| {
        match msg {
            Ok(ev) => {
                let json = serde_json::to_string(&ev).unwrap_or_else(|_| "{}".to_string());
                Ok(axum::response::sse::Event::default()
                    .event(ev.kind)
                    .data(json))
            }
            Err(_) => {
                // lagged/closed：发一个轻量事件也行，或者继续发 ping
                Ok(axum::response::sse::Event::default()
                    .event("lagged")
                    .data("1"))
            }
        }
    });

    Sse::new(heartbeat.merge(events)).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(20))
            .text("keepalive"),
    )
}

fn extract_tags(text: &str) -> Vec<String> {
    // 简单版：匹配 #xxx，允许字母数字下划线
    // 你后面可以升级：支持中文 tag、去重、大小写归一等
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
