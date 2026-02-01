# Home Ops Copilot

Backend + admin console for a local-first household operations assistant.

## Support Functions

### List tools (agent-available)
- `create_list(name: string)`
- `add_item(list_name: string, item: string)`
- `get_list(list_name: string)`

### Reminders (HTTP)
- `POST /reminders` create a reminder
- `GET /reminders` list reminders (`?active_only=true`)
- `GET /reminders/{id}` fetch a reminder
- `PATCH /reminders/{id}` update a reminder
- `POST /reminders/{id}/complete` complete a reminder
- `DELETE /reminders/{id}` delete a reminder

### Chat (HTTP)
- `POST /chat` send a message (supports optional `thread_id`)

### Calendar (HTTP)
- `POST /calendar/events` list events for a time range
- `GET /calendar/events/{id}` get a single event

#### Calendar OAuth setup (read-only)
- Set `CALENDAR_CREDENTIALS_PATH` to your Google OAuth `credentials.json` (default `credentials.json`)
- First request triggers local OAuth flow and stores token in `apps/api/data/token.json`
- Optional: set `CALENDAR_ID` (default `primary`)

## Calendar MCP Agent (local)
The MCP Python SDK requires Python 3.10+. Run this agent in a separate venv:

```bash
python3.11 -m venv .venv-mcp
source .venv-mcp/bin/activate
pip install mcp
python apps/calendar_agent/server.py
```

Set `MCP_TRANSPORT` to `stdio` (default) or `streamable-http` when you want to
serve it locally over HTTP. Use the same `CALENDAR_CREDENTIALS_PATH` and
`CALENDAR_TOKEN_PATH` environment variables as the API.

## Bowling MCP Agent (local)
This agent syncs bowling stats/schedules and exposes league tools.

Config:
- Create `apps/api/data/bowling.json` (see `packages/core/bowling/sample_config.json`)
- Set `BOWLING_CONFIG_PATH` if you store the file elsewhere

Run:
```bash
python3.11 -m venv .venv-mcp
source .venv-mcp/bin/activate
pip install mcp
python apps/bowling_agent/server.py
```

Tools:
- `list_leagues()`
- `sync_league(league_key)`
- `list_teams(league_key)`
- `team_stats(league_key, team_name)`
- `player_stats(league_key, player_name)`
- `list_matches(league_key, team_name?, date_from?, date_to?)`

## Logging
Standard format: `timestamp level app message`

### API
- `LOG_LEVEL` (default `INFO`)
- `LOG_DESTINATION` (`stdout`, `stderr`, or `file`)
- `LOG_FILE` required when `LOG_DESTINATION=file`

### Web
- `NEXT_PUBLIC_LOG_LEVEL` (default `info`)
- `NEXT_PUBLIC_LOG_DESTINATION` (`console`)

## Google OAuth (single-user)
Endpoints:
- `GET /api/integrations/google/start`
- `GET /api/integrations/google/callback`
- `GET /api/integrations/google/status`
- `POST /api/integrations/google/disconnect`

Gmail endpoints:
- `GET /api/integrations/gmail/unread?limit=10&query=is:unread`
- `GET /api/integrations/gmail/messages/{id}`

Calendar endpoints:
- `GET /api/integrations/calendar/upcoming?limit=10&from_iso=...`
- `POST /api/integrations/calendar/events`

Environment:
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REDIRECT_URI`
- `GOOGLE_OAUTH_SCOPES` (space-delimited)
- `GOOGLE_TOKEN_PATH` (default `apps/api/data/google_tokens.json`)
- `GOOGLE_STATE_PATH` (default `apps/api/data/google_state.json`)
- `GOOGLE_OAUTH_SUCCESS_REDIRECT` (default `http://localhost:3000`)
- `CALENDAR_SCOPES` (optional override for calendar-only scope)

