# Development script with auto-reload
$env_file = ".env"
$host_val = if (Select-String -Path $env_file -Pattern "^APP_HOST=") { (Select-String -Path $env_file -Pattern "^APP_HOST=(.+)").Matches[0].Groups[1].Value.Trim() } else { "0.0.0.0" }
$port_val = if (Select-String -Path $env_file -Pattern "^APP_PORT=") { (Select-String -Path $env_file -Pattern "^APP_PORT=(.+)").Matches[0].Groups[1].Value.Trim() } else { "8000" }
uv run uvicorn main:app --reload --app-dir worker --host $host_val --port $port_val --reload-include "*.py" --reload-include ".env"
