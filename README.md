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

## Logging
Standard format: `timestamp level app message`

### API
- `LOG_LEVEL` (default `INFO`)
- `LOG_DESTINATION` (`stdout`, `stderr`, or `file`)
- `LOG_FILE` required when `LOG_DESTINATION=file`

### Web
- `NEXT_PUBLIC_LOG_LEVEL` (default `info`)
- `NEXT_PUBLIC_LOG_DESTINATION` (`console`)

