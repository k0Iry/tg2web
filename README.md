# tg2web (webhook part)

I am opensourcing this part I am using for building a live website. A lightweight Telegram channel webhook receiver for tg2web.

This service accepts Telegram Bot API webhook updates, validates the secret token, extracts channel post data, normalizes it into a compact internal event format, and forwards it to a downstream queue for asynchronous processing.

It is designed to stay fast on the request path:
- validate
- parse
- normalize
- enqueue
- return `200 OK`

That keeps Telegram webhook delivery responsive and avoids request timeouts.

## What it does

- Accepts Telegram webhook updates
- Supports:
  - `channel_post`
  - `edited_channel_post`
- Extracts:
  - text / caption
  - tags
  - media
  - media groups
  - reply preview
  - entities
- Emits normalized events like:
  - `new_post`
  - `edited_post`

## Why this exists

Telegram webhooks should return quickly.

Instead of doing database writes, rendering, and downstream side effects directly inside the webhook request, this service only performs lightweight validation + normalization, then pushes the result to a queue.

That makes the system:
- more reliable
- easier to scale
- less likely to hit Telegram timeouts

## Event shape

The webhook transforms Telegram updates into an internal event like:

```json
{
  "kind": "new_post",
  "post": {
    "id": "tg:-1001234567890:42",
    "chat_id": -1001234567890,
    "message_id": 42,
    "date": 1710000000,
    "text": "hello world",
    "tags": ["example"],
    "edited": false,
    "received_at": "2026-04-01T12:00:00Z",
    "media": [],
    "media_group_id": null,
    "reply": null,
    "entities": null
  }
}
