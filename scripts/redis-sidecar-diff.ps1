# R4-1 scaffold (PowerShell): allowlist diff vs a Redis 8 sidecar (r4-1-cases.txt).
# NOT a full compatibility suite. Does not claim Redis parity.
# Covers stable String/Hash/List/Set/ZSet + TTL + Stream/Geo/Bitops/HLL lite.
# Out of scope: modules, DUMP/RESTORE, gossip, ACL, cluster, FT.*, FUNCTIONS,
# unordered replies (SMEMBERS/HGETALL), SCAN, exact remaining TTL seconds.
# Markers: @skip / @todo document gaps (not executed; do not fake pass).
param(
    [switch]$SelfCheck,
    [string]$CasesPath = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $CasesPath) {
    if ($env:R41_CASES) { $CasesPath = $env:R41_CASES } else { $CasesPath = Join-Path $ScriptDir "r4-1-cases.txt" }
}
$RedisHost = if ($env:REDIS_HOST) { $env:REDIS_HOST } else { "127.0.0.1" }
$RedisPort = if ($env:REDIS_PORT) { $env:REDIS_PORT } else { "6379" }
$GodisHost = if ($env:GODIS_HOST) { $env:GODIS_HOST } else { "127.0.0.1" }
$GodisPort = if ($env:GODIS_PORT) { $env:GODIS_PORT } else { "6399" }
$Cli = if ($env:REDISCLI) { $env:REDISCLI } else { "redis-cli" }
$Id = if ($env:R41_ID) { $env:R41_ID } else { "$PID" }

if (-not (Test-Path -LiteralPath $CasesPath)) {
    throw "cases file not found: $CasesPath"
}

function Get-Allowlist {
    param([string]$Path)
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match '@allowlist\s+(.+)$') {
            return $Matches[1].Trim()
        }
    }
    throw "missing @allowlist header in $Path"
}

$Allowlist = Get-Allowlist -Path $CasesPath

function Invoke-RedisCli {
    param([string]$HostName, [string]$Port, [Parameter(ValueFromRemainingArguments = $true)][string[]]$CliArgs)
    & $Cli --raw -h $HostName -p $Port @CliArgs
}

function Expand-CaseTokens {
    param([string]$Text)
    return $Text.Replace('{{ID}}', $Id)
}

function Test-Want {
    param([string]$Want, [string]$Got)
    if ($Want -match '^>=(-?\d+)$') {
        $n = [int64]0
        if (-not [int64]::TryParse($Got, [ref]$n)) { return $false }
        return $n -ge [int64]$Matches[1]
    }
    if ($Want -match '^<=(-?\d+)$') {
        $n = [int64]0
        if (-not [int64]::TryParse($Got, [ref]$n)) { return $false }
        return $n -le [int64]$Matches[1]
    }
    return $Got -eq $Want
}

function Expand-WantEscapes {
    param([string]$Want)
    # Literal \n in cases → real newline (multi-line --raw, e.g. ZPOPMIN).
    return $Want.Replace('\n', "`n")
}

function Normalize-CliOut {
    param([string]$Text)
    if ($null -eq $Text) { return "" }
    $s = $Text -replace "`r`n", "`n" -replace "`r", "`n"
    return $s.Trim()
}

function Fail-Cmp {
    param([string]$Label, [string]$Want, [string]$Rv, [string]$Gv, [string[]]$Cmd)
    $msg = @"
FAIL $Label
  cmd:   $($Cmd -join ' ')
  redis: $Rv
  godis: $Gv
  want:  $Want
"@
    throw $msg.TrimEnd()
}

if ($SelfCheck) {
    $cmd = Get-Command $Cli -ErrorAction SilentlyContinue
    if (-not $cmd) {
        Write-Host "R4-1 selfcheck: $Cli not on PATH (install later; allowlist still documented)"
        exit 0
    }
    Write-Host "R4-1 selfcheck ok: cases=$(Split-Path -Leaf $CasesPath); allowlist=$Allowlist; full suite (FT/modules/DUMP/cluster) out of scope"
    exit 0
}

Write-Host "R4-1 scaffold: allowlist-only via $CasesPath ($Allowlist). Full module/DUMP/gossip/FT/cluster diffs are out of scope."

$asserted = 0
foreach ($rawLine in Get-Content -LiteralPath $CasesPath) {
    $line = Expand-CaseTokens $rawLine.TrimEnd("`r")
    if ([string]::IsNullOrWhiteSpace($line) -or $line.TrimStart().StartsWith("#")) { continue }
    if ($line -match '^@allowlist') { continue }

    if ($line -match '^@(skip|todo)(\s|$)') {
        continue
    }

    if ($line.StartsWith("@")) {
        $parts = $line.Substring(1).Trim() -split '\s+'
        try { Invoke-RedisCli $RedisHost $RedisPort @parts | Out-Null } catch { }
        try { Invoke-RedisCli $GodisHost $GodisPort @parts | Out-Null } catch { }
        continue
    }

    $bits = $line -split '\|', 3
    if ($bits.Count -ne 3) { throw "bad case line: $rawLine" }
    $label = $bits[0]
    $want = Expand-WantEscapes $bits[1]
    $cmdParts = $bits[2].Trim() -split '\s+'
    if (-not $label -or -not $want -or $cmdParts.Count -eq 0) { throw "bad case line: $rawLine" }

    $rv = Normalize-CliOut ((@(Invoke-RedisCli $RedisHost $RedisPort @cmdParts) -join "`n"))
    $gv = Normalize-CliOut ((@(Invoke-RedisCli $GodisHost $GodisPort @cmdParts) -join "`n"))

    $ok = $false
    if ($want -match '^[<>]=') {
        $ok = (Test-Want $want $rv) -and (Test-Want $want $gv)
    } else {
        $ok = ($rv -eq $want) -and ($gv -eq $want)
    }
    if (-not $ok) {
        Fail-Cmp -Label $label -Want $want -Rv $rv -Gv $gv -Cmd $cmdParts
    }
    $asserted++
}

Write-Host "ran $asserted assertions from $(Split-Path -Leaf $CasesPath)"
Write-Host "allowlist diff passed (scaffolding only; see docs/COMPATIBILITY.md R4-1)"
