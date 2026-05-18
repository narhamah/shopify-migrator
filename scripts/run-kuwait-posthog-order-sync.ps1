param(
  [string]$Since
)

$ErrorActionPreference = "Stop"

$envPath = "C:\Users\narha\shopify-migrator\.env"
$repoPath = "C:\Users\narha\tara-kuwait-shopify"
$logPath = Join-Path $env:USERPROFILE "tara-kuwait-posthog-order-sync.log"

if (-not (Test-Path -LiteralPath $envPath)) {
  throw "Missing env file: $envPath"
}

Get-Content -LiteralPath $envPath | ForEach-Object {
  if ($_ -match "^\s*#" -or $_ -notmatch "=") { return }
  $parts = $_ -split "=", 2
  $name = $parts[0].Trim()
  $value = $parts[1].Trim().Trim('"').Trim("'")
  if ($name) {
    [Environment]::SetEnvironmentVariable($name, $value, "Process")
  }
}

$env:KUWAIT_SHOPIFY_SHOP = $env:DEST_SHOP_URL
$env:KUWAIT_SHOPIFY_ADMIN_TOKEN = $env:DEST_ACCESS_TOKEN
$env:KUWAIT_SHOPIFY_API_VERSION = if ($env:KUWAIT_ADMIN_API_VERSION) { $env:KUWAIT_ADMIN_API_VERSION } else { "2026-01" }
$env:KUWAIT_POSTHOG_PERSONAL_API_KEY = $env:POSTHOG_PERSONAL_API_KEY
$env:KUWAIT_POSTHOG_PROJECT_ID = if ($env:KUWAIT_POSTHOG_PROJECT_ID) { $env:KUWAIT_POSTHOG_PROJECT_ID } else { "429716" }
$env:KUWAIT_POSTHOG_API_HOST = if ($env:KUWAIT_POSTHOG_API_HOST) { $env:KUWAIT_POSTHOG_API_HOST } else { "https://us.posthog.com" }

if (-not $Since) {
  $Since = (Get-Date).ToUniversalTime().AddDays(-7).ToString("yyyy-MM-ddTHH:mm:ssZ")
}

Push-Location -LiteralPath $repoPath
try {
  $stamp = (Get-Date).ToString("s")
  "[$stamp] Running Kuwait PostHog order sync since $Since" | Out-File -FilePath $logPath -Append -Encoding utf8
  node ".\scripts\posthog-shopify-order-sync.mjs" --since="$Since" 2>&1 |
    Tee-Object -FilePath $logPath -Append
} finally {
  Pop-Location
}
