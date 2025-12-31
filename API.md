# ABC Crawler API Documentation

## Base URL
```
http://localhost:5500
```

## Table of Contents
- [Web Interface](#web-interface)
- [Process Management](#process-management)
- [URL Management](#url-management)
- [MIME Type Management](#mime-type-management)
- [Refused Extensions](#refused-extensions)
- [Host Management](#host-management)
- [Statistics](#statistics)

---

## Web Interface

### GET /
Serves the main web interface.

**Response:**
- HTML page with crawler control interface

---

## Process Management

### GET /api/processes
Get status of all running processes.

**Response:**
```json
{
  "dispatcher": {
    "pid": 1234,
    "status": "running"
  },
  "purger": {
    "pid": 5678,
    "status": "running"
  },
  "fetchers": [
    {
      "id": "1",
      "pid": 9012,
      "status": "running"
    }
  ],
  "parsers": [
    {
      "id": "1",
      "pid": 3456,
      "status": "running"
    }
  ]
}
```

### POST /api/processes/dispatcher/start
Start the URL dispatcher process.

**Response:**
```json
{
  "status": "ok",
  "pid": 1234
}
```

**Error Response:**
```json
{
  "status": "error",
  "message": "Dispatcher already running"
}
```

### POST /api/processes/dispatcher/stop
Stop the URL dispatcher process.

**Response:**
```json
{
  "status": "ok"
}
```

### POST /api/processes/purger/start
Start the URL purger process.

**Response:**
```json
{
  "status": "ok",
  "pid": 5678
}
```

### POST /api/processes/purger/stop
Stop the URL purger process.

**Response:**
```json
{
  "status": "ok"
}
```

### POST /api/processes/fetcher/add
Add a new fetcher process.

**Request Body (optional):**
```json
{
  "id": "2"
}
```

**Response:**
```json
{
  "status": "ok",
  "id": "1",
  "pid": 9012
}
```

### POST /api/processes/fetcher/<fetcher_id>/remove
Remove a specific fetcher process.

**Parameters:**
- `fetcher_id` (path): ID of the fetcher to remove

**Response:**
```json
{
  "status": "ok"
}
```

### POST /api/processes/parser/add
Add a new parser process.

**Request Body (optional):**
```json
{
  "id": "2"
}
```

**Response:**
```json
{
  "status": "ok",
  "id": "1",
  "pid": 3456
}
```

### POST /api/processes/parser/<parser_id>/remove
Remove a specific parser process.

**Parameters:**
- `parser_id` (path): ID of the parser to remove

**Response:**
```json
{
  "status": "ok"
}
```

### POST /api/processes/stop-all
Stop all running processes (dispatcher, purger, all fetchers, and all parsers).

**Response:**
```json
{
  "status": "ok",
  "stopped": [
    "fetcher-1",
    "fetcher-2",
    "parser-1",
    "purger",
    "dispatcher"
  ],
  "errors": null
}
```

**Response with Errors:**
```json
{
  "status": "ok",
  "stopped": ["fetcher-1", "parser-1"],
  "errors": [
    "fetcher-2: No such process",
    "dispatcher: Permission denied"
  ]
}
```

---

## URL Management

### GET /api/urls
Get a paginated list of URLs.

**Query Parameters:**
- `page` (optional, default: 1): Page number
- `per_page` (optional, default: 50): Items per page
- `status` (optional): Filter by status ('', 'dispatched', 'fetched', 'parsing', 'parsed', 'error')
- `host` (optional): Filter by hostname
- `mime_type` (optional): Filter by MIME type
- `has_abc` (optional): Filter by ABC content presence (true/false)
- `sort` (optional, default: 'created_at'): Sort field
- `order` (optional, default: 'desc'): Sort order ('asc' or 'desc')

**Response:**
```json
{
  "urls": [
    {
      "id": 1,
      "url": "https://example.com/page",
      "host": "example.com",
      "status": "parsed",
      "created_at": "2026-01-01 00:00:00",
      "downloaded_at": "2026-01-01 00:01:00",
      "size_bytes": 12345,
      "mime_type": "text/html",
      "http_status": 200,
      "retries": 0,
      "has_abc": true
    }
  ],
  "total": 1000,
  "page": 1,
  "per_page": 50,
  "total_pages": 20
}
```

### POST /api/urls
Add one or more URLs to the crawler queue.

**Request Body:**
```json
{
  "urls": [
    "https://example.com/page1",
    "https://example.com/page2"
  ]
}
```

**Response:**
```json
{
  "status": "ok",
  "added": 2,
  "duplicates": 0
}
```

### DELETE /api/urls/<url_id>
Delete a specific URL.

**Parameters:**
- `url_id` (path): ID of the URL to delete

**Response:**
```json
{
  "status": "ok"
}
```

---

## MIME Type Management

### GET /api/mime-types
Get all configured MIME types.

**Response:**
```json
{
  "mime_types": [
    {
      "id": 1,
      "mime_type": "text/html",
      "allowed": true
    },
    {
      "id": 2,
      "mime_type": "application/pdf",
      "allowed": false
    }
  ]
}
```

### POST /api/mime-types
Add a new MIME type configuration.

**Request Body:**
```json
{
  "mime_type": "text/plain",
  "allowed": true
}
```

**Response:**
```json
{
  "status": "ok",
  "id": 3
}
```

### PUT /api/mime-types/<mime_id>
Update an existing MIME type configuration.

**Parameters:**
- `mime_id` (path): ID of the MIME type to update

**Request Body:**
```json
{
  "allowed": false
}
```

**Response:**
```json
{
  "status": "ok"
}
```

### DELETE /api/mime-types/<mime_id>
Delete a MIME type configuration.

**Parameters:**
- `mime_id` (path): ID of the MIME type to delete

**Response:**
```json
{
  "status": "ok"
}
```

---

## Refused Extensions

### GET /api/refused-extensions
Get all refused file extensions.

**Response:**
```json
{
  "extensions": [
    {
      "extension": "exe",
      "created_at": "2026-01-01 00:00:00"
    },
    {
      "extension": "zip",
      "created_at": "2026-01-01 00:00:00"
    }
  ]
}
```

### POST /api/refused-extensions
Add a new refused extension.

**Request Body:**
```json
{
  "extension": "dmg"
}
```

**Response:**
```json
{
  "status": "ok"
}
```

### DELETE /api/refused-extensions/<extension>
Remove a refused extension.

**Parameters:**
- `extension` (path): Extension to remove (without dot)

**Response:**
```json
{
  "status": "ok"
}
```

---

## Host Management

### GET /api/hosts
Get all hosts with their statistics.

**Query Parameters:**
- `disabled_only` (optional): Show only disabled hosts (true/false)

**Response:**
```json
{
  "hosts": [
    {
      "host": "example.com",
      "last_access": "2026-01-01 00:00:00",
      "last_http_status": 200,
      "downloads": 150,
      "disabled": false,
      "disabled_reason": null,
      "disabled_at": null
    }
  ]
}
```

### PUT /api/hosts/<host>
Update host configuration (enable/disable).

**Parameters:**
- `host` (path): Hostname to update

**Request Body:**
```json
{
  "disabled": true,
  "disabled_reason": "manual"
}
```

**Response:**
```json
{
  "status": "ok"
}
```

---

## Statistics

### GET /api/stats
Get overall crawler statistics.

**Response:**
```json
{
  "total_urls": 10000,
  "by_status": {
    "": 5000,
    "dispatched": 100,
    "fetched": 200,
    "parsing": 50,
    "parsed": 4500,
    "error": 150
  },
  "total_size_bytes": 1234567890,
  "top_mime_types": {
    "text/html": 8000,
    "text/plain": 1500,
    "application/pdf": 500
  },
  "total_parsed": 4500,
  "total_with_abc": 1200,
  "total_tunebooks": 800,
  "total_tunes": 3500
}
```

---

## Error Responses

All endpoints may return error responses in the following format:

```json
{
  "status": "error",
  "message": "Error description"
}
```

Common HTTP status codes:
- `200 OK`: Request successful
- `400 Bad Request`: Invalid request parameters
- `404 Not Found`: Resource not found
- `500 Internal Server Error`: Server error

---

## Notes

- All timestamps are in the format `YYYY-MM-DD HH:MM:SS`
- The API uses JSON for request and response bodies
- Process management endpoints require the Flask app to have permissions to start/stop processes
- URL status flow: `''` (new) → `dispatched` → `fetched` → `parsing` → `parsed`
