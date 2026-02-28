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
use serde::{de::Deserializer, Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    secret_token: String,
    tx: mpsc::Sender<QueuedEvent>,
}

#[derive(Debug, Clone)]
struct QueuedEvent {
    body: Vec<u8>,
    sb_message_id: String,
    // 用来打日志定位
    update_id: i64,
    chat_id: i64,
    message_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Media {
    kind: String, // "photo" | "video" | "document"
    file_id: String,
    file_unique_id: Option<String>,
    mime_type: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    duration: Option<i32>,
    file_name: Option<String>,
    file_size: Option<i64>,
}

/// --- 业务数据结构（与你现在一致） ---

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
    #[serde(default)]
    media: Vec<Media>,
    #[serde(default)]
    media_group_id: Option<String>,
    #[serde(default)]
    reply: Option<TgReplyMessage>,
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
    // ✅ album 多图/多媒体会有
    #[serde(default)]
    media_group_id: Option<String>,

    // ✅ photo 是数组：从小到大多张缩略图
    #[serde(default)]
    photo: Option<Vec<TgPhotoSize>>,

    #[serde(default)]
    video: Option<TgVideo>,

    #[serde(default)]
    document: Option<TgDocument>,

    #[serde(default)]
    reply_to_message: Option<TgReplyMessage>,
}

#[derive(Debug, Clone, Serialize)]
struct TgReplyMessage {
    message_id: i64,
    text: Option<String>,
    caption: Option<String>,
    media_kind: Option<String>, // "photo" | "video" | "document"
}

// 自定义反序列化：根据实际媒体推断 kind，避免携带完整媒体体积
impl<'de> Deserialize<'de> for TgReplyMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawReply {
            message_id: i64,
            #[serde(default)]
            text: Option<String>,
            #[serde(default)]
            caption: Option<String>,
            #[serde(default)]
            photo: Option<Vec<TgPhotoSize>>,
            #[serde(default)]
            video: Option<TgVideo>,
            #[serde(default)]
            document: Option<TgDocument>,
        }

        let raw = RawReply::deserialize(deserializer)?;
        let media_kind = if raw.photo.as_ref().is_some_and(|v| !v.is_empty()) {
            Some("photo".to_string())
        } else if raw.video.is_some() {
            Some("video".to_string())
        } else if raw.document.is_some() {
            Some("document".to_string())
        } else {
            None
        };

        Ok(TgReplyMessage {
            message_id: raw.message_id,
            text: raw.text,
            caption: raw.caption,
            media_kind,
        })
    }
}

#[derive(Debug, Deserialize)]
struct TgPhotoSize {
    file_id: String,
    #[serde(default)]
    file_unique_id: Option<String>,
    #[serde(default)]
    width: Option<i32>,
    #[serde(default)]
    height: Option<i32>,
    #[serde(default)]
    file_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TgVideo {
    file_id: String,
    #[serde(default)]
    file_unique_id: Option<String>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    width: Option<i32>,
    #[serde(default)]
    height: Option<i32>,
    #[serde(default)]
    duration: Option<i32>,
    #[serde(default)]
    file_name: Option<String>,
    #[serde(default)]
    file_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TgDocument {
    file_id: String,
    #[serde(default)]
    file_unique_id: Option<String>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    file_name: Option<String>,
    #[serde(default)]
    file_size: Option<i64>,
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
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "info,azservicebus=warn,tower_http=info".to_string()),
        )
        .init();

    let secret_token = std::env::var("TELEGRAM_WEBHOOK_SECRET")?;
    let conn = std::env::var("SERVICEBUS_CONNECTION_STRING")
        .expect("missing SERVICEBUS_CONNECTION_STRING");
    let topic = std::env::var("SERVICEBUS_TOPIC").unwrap_or_else(|_| "posts".to_string());

    // ✅ 有界队列：避免无限吃内存
    let queue_cap: usize = std::env::var("QUEUE_CAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    let (tx, rx) = mpsc::channel::<QueuedEvent>(queue_cap);

    // ✅ 启动后台 publisher（慢慢发到 SB）
    tokio::spawn(async move {
        publisher_loop(conn, topic, rx).await;
    });

    let state = AppState { secret_token, tx };

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
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// ✅ webhook：只做校验/解析/入队，然后立刻 200
async fn telegram_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(update): Json<TgUpdate>,
) -> impl IntoResponse {
    // 1) 校验 secret token
    if headers
        .get("x-telegram-bot-api-secret-token")
        .and_then(|v| v.to_str().ok())
        .is_none_or(|token| token != state.secret_token)
    {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    // 2) 取 message
    let (msg, edited) = match (update.channel_post, update.edited_channel_post) {
        (Some(m), _) => (m, false),
        (None, Some(m)) => (m, true),
        (None, None) => return (StatusCode::OK, "ignored").into_response(),
    };

    // 3) 提取文本
    let media = extract_media(&msg);
    let text = msg.text.or(msg.caption).unwrap_or_default();

    // ✅ 只有文字和 media 都空，才忽略
    if text.trim().is_empty() && media.is_empty() {
        return (StatusCode::OK, "ignored").into_response();
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

        media,
        media_group_id: msg.media_group_id.clone(),
        reply: msg.reply_to_message.clone(),
    };

    let kind = if edited { "edited_post" } else { "new_post" }.to_string();
    let ev = PostEvent { kind, post };

    // 4) 序列化
    let body = match serde_json::to_vec(&ev) {
        Ok(b) => b,
        Err(e) => {
            warn!("serialize failed: {:?}", e);
            return (StatusCode::BAD_REQUEST, "bad_payload").into_response();
        }
    };

    // 5) SB message_id：用 update_id 保证唯一（避免 duplicate detection 吞 edited）
    let sb_message_id = format!("{}:u{}", ev.post.id, update.update_id);

    // 6) 入队（快）
    let q = QueuedEvent {
        body,
        sb_message_id,
        update_id: update.update_id,
        chat_id: ev.post.chat_id,
        message_id: ev.post.message_id,
    };

    // 队列满了：返回 503，让 Telegram 重试（避免静默丢）
    if let Err(_e) = state.tx.try_send(q) {
        warn!(
            "queue full, returning 503 for retry (update_id={}, chat_id={}, msg_id={})",
            update.update_id, ev.post.chat_id, ev.post.message_id
        );
        return (StatusCode::SERVICE_UNAVAILABLE, "queue_full").into_response();
    }

    info!(
        "enqueued update_id={} chat_id={} msg_id={} edited={}",
        update.update_id, ev.post.chat_id, ev.post.message_id, ev.post.edited
    );

    // ✅ 立刻返回 200，彻底消灭 Telegram read timeout
    (StatusCode::OK, "ok").into_response()
}

/// 后台 loop：从队列拿 -> 发到 Service Bus
async fn publisher_loop(conn: String, topic: String, mut rx: mpsc::Receiver<QueuedEvent>) {
    info!("publisher_loop started: topic={}", topic);

    while let Some(item) = rx.recv().await {
        // 对每条消息做“直到成功”的重试，但带上退避，避免打爆
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;

            match publish_once(
                &conn,
                &topic,
                item.body.clone(),
                item.sb_message_id.clone(),
                item.chat_id,
            )
            .await
            {
                Ok(_) => {
                    info!(
                        "published ok (attempt={} update_id={} chat_id={} msg_id={})",
                        attempt, item.update_id, item.chat_id, item.message_id
                    );
                    break;
                }
                Err(e) => {
                    // 指数退避：100ms -> ... -> 最大 5s
                    let backoff_ms = (100u64 * (1u64 << (attempt.min(6) - 1))).min(5000);
                    warn!(
                        "publish failed (attempt={} backoff={}ms update_id={} msg_id={}): {:?}",
                        attempt, backoff_ms, item.update_id, item.message_id, e
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
            }
        }
    }

    warn!("publisher_loop exited: receiver closed");
}

/// 每次 publish 都新建 client/sender：无共享状态，最稳
async fn publish_once(
    conn: &str,
    topic: &str,
    body: Vec<u8>,
    message_id: String,
    chat_id: i64,
) -> anyhow::Result<()> {
    let mut client: ServiceBusClient<BasicRetryPolicy> =
        ServiceBusClient::new_from_connection_string(
            conn.to_string(),
            ServiceBusClientOptions::default(),
        )
        .await?;

    let mut sender = client
        .create_sender(topic.to_string(), ServiceBusSenderOptions::default())
        .await?;

    let mut msg = ServiceBusMessage::new(body);
    msg.set_message_id(message_id)?;
    msg.set_session_id(chat_id.to_string())?;
    msg.set_partition_key(chat_id.to_string())?;
    sender.send_message(msg).await?;
    Ok(())
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

fn extract_media(msg: &TgMessage) -> Vec<Media> {
    let mut out = Vec::new();

    // photo: 取最大那张（最后一张通常分辨率最大）
    if let Some(list) = &msg.photo {
        if let Some(p) = list.last() {
            out.push(Media {
                kind: "photo".to_string(),
                file_id: p.file_id.clone(),
                file_unique_id: p.file_unique_id.clone(),
                mime_type: None,
                width: p.width,
                height: p.height,
                duration: None,
                file_name: None,
                file_size: p.file_size,
            });
        }
    }

    if let Some(v) = &msg.video {
        out.push(Media {
            kind: "video".to_string(),
            file_id: v.file_id.clone(),
            file_unique_id: v.file_unique_id.clone(),
            mime_type: v.mime_type.clone(),
            width: v.width,
            height: v.height,
            duration: v.duration,
            file_name: v.file_name.clone(),
            file_size: v.file_size,
        });
    }

    if let Some(d) = &msg.document {
        out.push(Media {
            kind: "document".to_string(),
            file_id: d.file_id.clone(),
            file_unique_id: d.file_unique_id.clone(),
            mime_type: d.mime_type.clone(),
            width: None,
            height: None,
            duration: None,
            file_name: d.file_name.clone(),
            file_size: d.file_size,
        });
    }

    out
}
