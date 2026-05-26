# ioBroker.iosync

ioBroker adapter that exposes selected data points via a secure HTTPS API for smartwatches and other apps.

## Features

- Expose selected ioBroker data points via HTTPS API
- Configurable polling intervals per data point
- Basic Auth protection
- Self-signed SSL certificate support
- Write ioBroker data points via `POST /api/setState`
- Direct reads via `GET /api/state/:id`

## Installation

In ioBroker Admin, go to **Adapters** → search for `iosync`, or install via GitHub URL:

```
https://github.com/thestealth131205/ioBroker.iosync
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `apiPort` | HTTPS port for the API | `7443` |
| `apiUsername` | Basic Auth username | `admin` |
| `apiPassword` | Basic Auth password | *(empty)* |
| `dataPoints` | List of ioBroker data point IDs to expose | `[]` |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/states` | Returns all configured data points |
| `GET` | `/api/state/:id` | Returns a single data point by ID |
| `POST` | `/api/setState` | Writes a value to an ioBroker data point |

### Example: Read all states

```
GET https://<iobroker-host>:7443/api/states
Authorization: Basic <base64(user:pass)>
```

### Example: Write a state

```
POST https://<iobroker-host>:7443/api/setState
Authorization: Basic <base64(user:pass)>
Content-Type: application/json

{ "id": "hm-rpc.0.ABC123.STATE", "value": true }
```

## License

MIT
