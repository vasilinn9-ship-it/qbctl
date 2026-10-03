$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
$daemonExe = Join-Path $repoRoot "target\debug\qbctld.exe"
$cliExe = Join-Path $repoRoot "target\debug\qbctl.exe"
$testId = [guid]::NewGuid().ToString("N")
$runtimeRoot = Join-Path ([System.IO.Path]::GetTempPath()) "qbctl-control-plane-$testId"
$pipeLeaf = "qbctl-acceptance-$testId"
$pipePath = "\\.\pipe\$pipeLeaf"

function New-ProcessInfo {
    param([string]$FileName, [string[]]$Arguments, [string]$Pipe = $pipePath)

    $info = [System.Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $FileName
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    foreach ($argument in $Arguments) {
        [void]$info.ArgumentList.Add($argument)
    }
    $info.Environment["QBCTL_PIPE"] = $Pipe
    return $info
}

function Start-Daemon {
    $info = New-ProcessInfo -FileName $daemonExe -Arguments @("run", "--runtime-dir", $runtimeRoot)
    return [System.Diagnostics.Process]::Start($info)
}

function Invoke-CliBytes {
    param([string[]]$Arguments, [string]$Pipe = $pipePath)

    $info = New-ProcessInfo -FileName $cliExe -Arguments $Arguments -Pipe $Pipe
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true

    $process = [System.Diagnostics.Process]::Start($info)
    $memory = [System.IO.MemoryStream]::new()
    $process.StandardOutput.BaseStream.CopyTo($memory)
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()

    return [pscustomobject]@{
        ExitCode = $process.ExitCode
        Stdout = $memory.ToArray()
        Stderr = $stderr
    }
}

function Invoke-CliText {
    param([string[]]$Arguments, [string]$Pipe = $pipePath)

    $result = Invoke-CliBytes -Arguments $Arguments -Pipe $Pipe
    return [pscustomobject]@{
        ExitCode = $result.ExitCode
        Stdout = [System.Text.Encoding]::UTF8.GetString($result.Stdout)
        Stderr = $result.Stderr
    }
}

function Wait-DaemonReady {
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $result = Invoke-CliText -Arguments @("status")
        if ($result.ExitCode -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 100
    }
    throw "daemon did not become ready"
}

function Stop-ProcessSafe {
    param([System.Diagnostics.Process]$Process)

    if ($null -ne $Process -and -not $Process.HasExited) {
        $Process.Kill($true)
        $Process.WaitForExit()
    }
}

function Read-Exact {
    param([System.IO.Stream]$Stream, [int]$Count)

    $buffer = [byte[]]::new($Count)
    $offset = 0
    while ($offset -lt $Count) {
        $read = $Stream.Read($buffer, $offset, $Count - $offset)
        if ($read -eq 0) {
            throw "unexpected end of pipe"
        }
        $offset += $read
    }
    return $buffer
}

function Write-Frame {
    param(
        [System.IO.Stream]$Stream,
        [byte[]]$Payload,
        [string]$Context = "frame"
    )

    try {
        $length = [System.BitConverter]::GetBytes([uint32]$Payload.Length)
        $Stream.Write($length, 0, $length.Length)
        if ($Payload.Length -gt 0) {
            $Stream.Write($Payload, 0, $Payload.Length)
        }
        $Stream.Flush()
    }
    catch {
        throw "$Context write failed: $($_.Exception.Message)"
    }
}

function Read-Frame {
    param([System.IO.Stream]$Stream)

    $lengthBytes = Read-Exact -Stream $Stream -Count 4
    $length = [System.BitConverter]::ToUInt32($lengthBytes, 0)
    if ($length -gt 16777216) {
        throw "test peer returned oversized frame"
    }
    return Read-Exact -Stream $Stream -Count ([int]$length)
}

function Connect-RawClient {
    param([string]$Leaf = $pipeLeaf)

    $client = [System.IO.Pipes.NamedPipeClientStream]::new(
        ".",
        $Leaf,
        [System.IO.Pipes.PipeDirection]::InOut,
        [System.IO.Pipes.PipeOptions]::None
    )
    $client.Connect(5000)
    return $client
}

function Assert-Exit {
    param([int]$Expected, $Result, [string]$Context)

    if ($Result.ExitCode -ne $Expected) {
        throw "$Context expected exit $Expected, got $($Result.ExitCode). stderr=$($Result.Stderr)"
    }
}

function Invoke-FakeServerScenario {
    param([byte[]]$ServerHello, [byte[]]$Response, [int]$ExpectedExit)

    $leaf = "qbctl-fake-$([guid]::NewGuid().ToString("N"))"
    $server = [System.IO.Pipes.NamedPipeServerStream]::new(
        $leaf,
        [System.IO.Pipes.PipeDirection]::InOut,
        1,
        [System.IO.Pipes.PipeTransmissionMode]::Byte,
        [System.IO.Pipes.PipeOptions]::Asynchronous
    )

    try {
        $wait = $server.BeginWaitForConnection($null, $null)
        $info = New-ProcessInfo -FileName $cliExe -Arguments @("status") -Pipe "\\.\pipe\$leaf"
        $info.RedirectStandardOutput = $true
        $info.RedirectStandardError = $true
        $process = [System.Diagnostics.Process]::Start($info)

        $server.EndWaitForConnection($wait)
        [void](Read-Frame -Stream $server)
        Write-Frame -Stream $server -Payload $ServerHello -Context "fake server hello"

        if ($null -ne $Response) {
            [void](Read-Frame -Stream $server)
            Write-Frame -Stream $server -Payload $Response -Context "fake server response"
        }

        $stderr = $process.StandardError.ReadToEnd()
        $process.StandardOutput.BaseStream.CopyTo([System.IO.Stream]::Null)
        $process.WaitForExit()

        if ($process.ExitCode -ne $ExpectedExit) {
            throw "fake server expected CLI exit $ExpectedExit, got $($process.ExitCode). stderr=$stderr"
        }
    }
    finally {
        $server.Dispose()
    }
}

New-Item -ItemType Directory -Path $runtimeRoot -Force | Out-Null
$daemon = $null

try {
    $unavailable = Invoke-CliText -Arguments @("status")
    Assert-Exit -Expected 6 -Result $unavailable -Context "daemon unavailable"

    $usage = Invoke-CliText -Arguments @("not-a-command")
    Assert-Exit -Expected 2 -Result $usage -Context "invalid command"

    $daemon = Start-Daemon
    Wait-DaemonReady

    Assert-Exit 0 (Invoke-CliText @("status")) "status"
    Assert-Exit 0 (Invoke-CliText @("capabilities")) "capabilities"
    Assert-Exit 0 (Invoke-CliText @("doctor")) "doctor"

    $torrentUnavailable = Invoke-CliText @("--output", "fields", "torrent", "list")
    Assert-Exit 6 $torrentUnavailable "torrent list without qBittorrent"
    if ($torrentUnavailable.Stdout -notmatch "(?m)^problem\.0\.code=QBIT_UNAVAILABLE\r?$") {
        throw "torrent list did not report QBIT_UNAVAILABLE"
    }

    $blockedPause = Invoke-CliText @(
        "--output", "fields",
        "torrent", "pause", "abcdef0123456789abcdef0123456789abcdef01",
        "--request-id", "acceptance-blocked-pause"
    )
    Assert-Exit 3 $blockedPause "torrent pause while daemon is degraded"
    if ($blockedPause.Stdout -notmatch "(?m)^problem\.0\.code=MUTATION_ADMISSION_DISABLED\r?$") {
        throw "degraded torrent pause did not fail closed"
    }

    $initialTarget = Invoke-CliText @("--output", "fields", "queue", "target", "get")
    Assert-Exit 0 $initialTarget "initial queue target"
    if ($initialTarget.Stdout -notmatch "(?m)^policy_revision=1\r?$") {
        throw "initial queue target revision is not 1"
    }

    $setTarget = Invoke-CliText @(
        "--output", "fields",
        "queue", "target", "set", "7",
        "--request-id", "acceptance-queue-target"
    )
    Assert-Exit 0 $setTarget "queue target set"
    if ($setTarget.Stdout -notmatch "(?m)^replayed=false\r?$") {
        throw "new queue target mutation was not reported as new"
    }

    $target = Invoke-CliText @("--output", "fields", "queue", "target", "get")
    Assert-Exit 0 $target "queue target get after set"
    if ($target.Stdout -notmatch "(?m)^policy_revision=2\r?$" -or
        $target.Stdout -notmatch "(?m)^target_client_count=7\r?$") {
        throw "queue target policy did not persist the requested value/revision"
    }

    $replayTarget = Invoke-CliText @(
        "--output", "fields",
        "queue", "target", "set", "7",
        "--request-id", "acceptance-queue-target"
    )
    Assert-Exit 0 $replayTarget "queue target replay"
    if ($replayTarget.Stdout -notmatch "(?m)^replayed=true\r?$") {
        throw "same RequestId/same queue target did not replay"
    }

    $conflictTarget = Invoke-CliText @(
        "--output", "fields",
        "queue", "target", "set", "8",
        "--request-id", "acceptance-queue-target"
    )
    Assert-Exit 3 $conflictTarget "queue target RequestId conflict"
    if ($conflictTarget.Stdout -notmatch "(?m)^problem\.0\.code=REQUEST_ID_CONFLICT\r?$") {
        throw "same RequestId/different queue target did not conflict"
    }

    $fields = Invoke-CliText @("--output", "fields", "status")
    Assert-Exit 0 $fields "fields status"
    if ($fields.Stdout -notmatch "(?m)^status=STATUS_OK\r?$") {
        throw "fields output missing STATUS_OK"
    }

    $proto = Invoke-CliBytes @("--output", "proto", "status")
    Assert-Exit 0 $proto "proto status"
    if ($proto.Stdout.Length -lt 4 -or
        $proto.Stdout[0] -ne 0x08 -or
        $proto.Stdout[1] -ne 0x01 -or
        $proto.Stdout[2] -ne 0x10 -or
        $proto.Stdout[3] -ne 0x01) {
        throw "--output proto is not a raw qbctl.v1.Response payload"
    }
    if ($proto.Stderr.Length -ne 0) {
        throw "--output proto emitted unexpected stderr: $($proto.Stderr)"
    }

    $second = Start-Daemon
    if (-not $second.WaitForExit(5000)) {
        Stop-ProcessSafe $second
        throw "duplicate daemon did not reject same runtime root"
    }
    if ($second.ExitCode -eq 0) {
        throw "duplicate daemon unexpectedly succeeded"
    }

    $raw = Connect-RawClient
    try {
        Write-Frame $raw ([byte[]](0x08, 0x01, 0x10, 0xFF, 0x01)) -Context "minor-version client hello"
        [void](Read-Frame $raw)
        Write-Frame $raw ([byte[]](0x08, 0x01, 0x5A, 0x00)) -Context "minor-version status request"
        $response = Read-Frame $raw
        if ($response.Length -lt 4 -or $response[0] -ne 0x08 -or $response[1] -ne 0x01) {
            throw "minor-version client did not receive a valid response"
        }
    }
    finally {
        $raw.Dispose()
    }

    $raw = Connect-RawClient
    try {
        Write-Frame $raw ([byte[]](0xFF)) -Context "malformed protobuf hello"
    }
    finally {
        $raw.Dispose()
    }
    Start-Sleep -Milliseconds 100
    Assert-Exit 0 (Invoke-CliText @("status")) "status after malformed protobuf"

    $raw = Connect-RawClient
    try {
        $prefix = [System.BitConverter]::GetBytes([uint32]4)
        $raw.Write($prefix, 0, $prefix.Length)
        $raw.WriteByte(0x08)
        $raw.Flush()
    }
    finally {
        $raw.Dispose()
    }
    Start-Sleep -Milliseconds 100
    Assert-Exit 0 (Invoke-CliText @("status")) "status after truncated frame"

    $raw = Connect-RawClient
    try {
        $prefix = [System.BitConverter]::GetBytes([uint32]16777217)
        $raw.Write($prefix, 0, $prefix.Length)
        $raw.Flush()
    }
    finally {
        $raw.Dispose()
    }
    Start-Sleep -Milliseconds 100
    Assert-Exit 0 (Invoke-CliText @("status")) "status after oversized frame"

    Stop-ProcessSafe $daemon
    $daemon = Start-Daemon
    Wait-DaemonReady
    Assert-Exit 0 (Invoke-CliText @("doctor")) "doctor after restart"

    $targetAfterRestart = Invoke-CliText @("--output", "fields", "queue", "target", "get")
    Assert-Exit 0 $targetAfterRestart "queue target after restart"
    if ($targetAfterRestart.Stdout -notmatch "(?m)^policy_revision=2\r?$" -or
        $targetAfterRestart.Stdout -notmatch "(?m)^target_client_count=7\r?$") {
        throw "queue target policy/revision did not survive daemon restart"
    }

    if (-not (Test-Path (Join-Path $runtimeRoot "state.sqlite"))) {
        throw "state.sqlite missing from configured runtime root"
    }
    if (-not (Test-Path (Join-Path $runtimeRoot "daemon.lock"))) {
        throw "daemon.lock missing from configured runtime root"
    }
    if (Test-Path (Join-Path $repoRoot "state.sqlite")) {
        throw "Rust runtime state leaked into source checkout"
    }

    Invoke-FakeServerScenario -ServerHello ([byte[]](0x08, 0x02)) -Response $null -ExpectedExit 7
    Invoke-FakeServerScenario -ServerHello ([byte[]](0x08, 0x01)) -Response ([byte[]](0x08, 0x01, 0x10, 0x06)) -ExpectedExit 8

    Write-Host "Control-plane acceptance: OK"
}
finally {
    Stop-ProcessSafe $daemon
    Remove-Item -Path $runtimeRoot -Recurse -Force -ErrorAction SilentlyContinue
}
