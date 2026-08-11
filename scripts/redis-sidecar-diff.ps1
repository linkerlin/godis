# R4-1 scaffold (PowerShell): tiny allowlist diff vs a Redis 8 sidecar.
# NOT a full compatibility suite. Does not claim Redis parity.
# Allowlist: PING / SET / GET / DEL / EXISTS / INCR / TYPE
param(
    [switch]$SelfCheck
)

$ErrorActionPreference = "Stop"
$RedisHost = if ($env:REDIS_HOST) { $env:REDIS_HOST } else { "127.0.0.1" }
$RedisPort = if ($env:REDIS_PORT) { $env:REDIS_PORT } else { "6379" }
$GodisHost = if ($env:GODIS_HOST) { $env:GODIS_HOST } else { "127.0.0.1" }
$GodisPort = if ($env:GODIS_PORT) { $env:GODIS_PORT } else { "6399" }
$Cli = if ($env:REDISCLI) { $env:REDISCLI } else { "redis-cli" }

function Invoke-RedisCli {
    param([string]$HostName, [string]$Port, [Parameter(ValueFromRemainingArguments = $true)][string[]]$Args)
    & $Cli -h $HostName -p $Port @Args
}

if ($SelfCheck) {
    $cmd = Get-Command $Cli -ErrorAction SilentlyContinue
    if (-not $cmd) { throw "selfcheck: $Cli not on PATH (ok to install later)" }
    Write-Host "R4-1 selfcheck ok: allowlist=PING,SET,GET,DEL,EXISTS,INCR,TYPE; full suite out of scope"
    exit 0
}

Write-Host "R4-1 scaffold: allowlist-only (PING/SET/GET/DEL/EXISTS/INCR/TYPE). Full module/DUMP/gossip diffs are out of scope."

function Assert-Both {
    param([string]$Label, [string]$Want, [string[]]$Cmd)
    $rv = (Invoke-RedisCli $RedisHost $RedisPort @Cmd | Out-String).Trim()
    $gv = (Invoke-RedisCli $GodisHost $GodisPort @Cmd | Out-String).Trim()
    if ($rv -ne $Want -or $gv -ne $Want) {
        throw "$Label mismatch: redis=$rv godis=$gv want=$Want"
    }
}

$rPing = (Invoke-RedisCli $RedisHost $RedisPort PING | Out-String).Trim()
$gPing = (Invoke-RedisCli $GodisHost $GodisPort PING | Out-String).Trim()
if ($rPing -ne "PONG" -or $gPing -ne "PONG") {
    throw "PING mismatch: redis=$rPing godis=$gPing"
}

$key = "sidecar:allowlist:$PID"
$val = "ok"
Invoke-RedisCli $RedisHost $RedisPort DEL $key | Out-Null
Invoke-RedisCli $GodisHost $GodisPort DEL $key | Out-Null

Assert-Both "SET" "OK" @("SET", $key, $val)
Assert-Both "GET" $val @("GET", $key)
Assert-Both "EXISTS" "1" @("EXISTS", $key)
Assert-Both "TYPE" "string" @("TYPE", $key)

$nkey = "sidecar:allowlist:n:$PID"
Invoke-RedisCli $RedisHost $RedisPort DEL $nkey | Out-Null
Invoke-RedisCli $GodisHost $GodisPort DEL $nkey | Out-Null
Assert-Both "INCR" "1" @("INCR", $nkey)
Assert-Both "INCR2" "2" @("INCR", $nkey)
Assert-Both "GET-n" "2" @("GET", $nkey)

Assert-Both "DEL" "1" @("DEL", $key)
Assert-Both "EXISTS-after-del" "0" @("EXISTS", $key)
Assert-Both "DEL-n" "1" @("DEL", $nkey)

Write-Host "allowlist diff passed (scaffolding only; see docs/COMPATIBILITY.md R4-1)"
