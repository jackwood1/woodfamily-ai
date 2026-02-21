# Newsletter Digest Agent

A Model Context Protocol (MCP) agent for managing newsletter subscriptions and generating AI-powered digests.

## Features

- **Newsletter Detection**: Automatically detect newsletters in Gmail
- **Subscription Management**: Subscribe/unsubscribe from newsletters
- **AI Summarization**: Use LLM to summarize newsletter content
- **Digest Generation**: Create periodic digests from subscribed newsletters
- **Configuration**: Customize digest schedule and preferences

## MCP Tools

### `detect_newsletters(limit, days_back)`
Scan Gmail for potential newsletters.

**Parameters:**
- `limit` (int): Maximum number of messages to scan (default: 50)
- `days_back` (int): How many days back to search (default: 7)

**Returns:** List of detected newsletters with sender info and snippets

### `subscribe_newsletter(sender_email, sender_name?)`
Add a newsletter to the digest subscription list.

**Parameters:**
- `sender_email` (str): Email address of the newsletter sender
- `sender_name` (str, optional): Name of the newsletter

**Returns:** Status object

### `unsubscribe_newsletter(sender_email)`
Remove a newsletter from subscriptions.

**Parameters:**
- `sender_email` (str): Email address to unsubscribe

**Returns:** Status object

### `list_subscriptions()`
List all newsletter subscriptions.

**Returns:** List of subscription objects with status

### `pause_subscription(sender_email)`
Temporarily pause a subscription without unsubscribing.

**Parameters:**
- `sender_email` (str): Email address to pause

**Returns:** Status object

### `resume_subscription(sender_email)`
Resume a paused subscription.

**Parameters:**
- `sender_email` (str): Email address to resume

**Returns:** Status object

### `generate_digest(since_date?, max_newsletters)`
Generate a digest from subscribed newsletters.

**Parameters:**
- `since_date` (str, optional): ISO date string (e.g., "2024-01-15"), defaults to 7 days ago
- `max_newsletters` (int): Maximum newsletters to include (default: 20)

**Returns:** Digest object with overall summary and individual newsletter summaries

### `list_digests(limit)`
List recently generated digests.

**Parameters:**
- `limit` (int): Number of digests to return (default: 10)

**Returns:** List of digest summaries

### `get_digest(digest_id)`
Get a specific digest with all newsletter summaries.

**Parameters:**
- `digest_id` (str): Digest ID

**Returns:** Full digest object with newsletters

### `get_digest_config()`
Get current digest configuration.

**Returns:** Configuration object with schedule and preferences

### `update_digest_config(schedule?, max_per_digest?, auto_generate?)`
Update digest configuration.

**Parameters:**
- `schedule` (str, optional): "daily", "weekly", or "manual"
- `max_per_digest` (int, optional): Max newsletters per digest
- `auto_generate` (bool, optional): Enable automatic digest generation

**Returns:** Status object

## Usage Examples

### Via Chat Agent

```
User: "Scan my Gmail for newsletters"
Agent: [calls detect_newsletters(limit=50, days_back=7)]

User: "Subscribe to newsletters from tech@example.com"
Agent: [calls subscribe_newsletter("tech@example.com")]

User: "Generate my weekly newsletter digest"
Agent: [calls generate_digest(since_date="2024-01-15", max_newsletters=20)]

User: "Show me my recent digests"
Agent: [calls list_digests(limit=5)]
```

## Running the Agent

The newsletter agent runs as an MCP server. It's automatically available to the main HomeOps agent through the MCP protocol.

### Environment Variables

- `HOME_OPS_DB_PATH`: Path to SQLite database (default: `apps/api/data/lists.db`)
- `MCP_TRANSPORT`: Transport mode (default: `stdio`)
- `OPENAI_API_KEY`: Required for newsletter summarization
- `OPENAI_MODEL`: LLM model to use (default: `gpt-4o-mini`)

## Architecture

```
Newsletter Agent
├── MCP Server (FastMCP)
│   └── Tools (detect, subscribe, generate, etc.)
└── Newsletter Service
    ├── Detector (Gmail newsletter detection)
    ├── Summarizer (LLM-based summarization)
    ├── Digest Generator (combine summaries)
    └── Storage (SQLite subscriptions & digests)
```

## Database Schema

### newsletter_subscriptions
- `id`: Subscription ID
- `sender_email`: Newsletter sender email (unique)
- `sender_name`: Newsletter name
- `status`: "active", "paused", or "ignored"
- `created_at`: Timestamp
- `updated_at`: Timestamp

### newsletter_digests
- `id`: Digest ID
- `period_start`: Start date (ISO)
- `period_end`: End date (ISO)
- `summary`: Overall digest summary
- `newsletter_count`: Number of newsletters
- `created_at`: Timestamp

### newsletter_summaries
- `id`: Summary ID
- `digest_id`: Foreign key to digest
- `message_id`: Gmail message ID
- `sender`: Newsletter sender
- `sender_email`: Sender email
- `subject`: Newsletter subject
- `summary`: AI-generated summary
- `received_date`: When newsletter was received

### newsletter_config
- `schedule`: "daily", "weekly", or "manual"
- `max_per_digest`: Max newsletters per digest
- `auto_generate`: Enable/disable auto-generation
- `updated_at`: Timestamp

## API Routes

The newsletter functionality is also available via REST API:

- `GET /api/newsletters/detect` - Detect newsletters
- `GET /api/newsletters/subscriptions` - List subscriptions
- `POST /api/newsletters/subscriptions` - Subscribe
- `DELETE /api/newsletters/subscriptions/{email}` - Unsubscribe
- `PATCH /api/newsletters/subscriptions/{email}/pause` - Pause
- `PATCH /api/newsletters/subscriptions/{email}/resume` - Resume
- `GET /api/newsletters/digests` - List digests
- `GET /api/newsletters/digests/{id}` - Get digest
- `POST /api/newsletters/digests/generate` - Generate digest
- `GET /api/newsletters/config` - Get config
- `PATCH /api/newsletters/config` - Update config

## Notes

- Requires Google OAuth authentication for Gmail access
- Newsletter detection uses heuristics (subject patterns, unsubscribe links, etc.)
- Summarization requires OpenAI API key
- Digests are stored indefinitely (can be retrieved later)
- Paused subscriptions won't appear in digests but remain in the system
