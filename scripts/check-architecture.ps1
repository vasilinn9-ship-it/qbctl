$ErrorActionPreference = "Stop"

$metadata = cargo metadata --format-version 1 --no-deps --locked | ConvertFrom-Json
$workspace = @{}
foreach ($package in $metadata.packages) {
    $workspace[$package.name] = $package
}

$allowed = @{
    "qb-domain"      = @()
    "qb-application" = @("qb-domain")
    "qb-proto"       = @()
    "qb-ipc"         = @()
    "qb-metainfo"    = @("qb-domain", "qb-application")
    "qb-qbit"        = @("qb-domain", "qb-application")
    "qb-journal"     = @("qb-domain", "qb-application")
    "qb-win"         = @("qb-domain", "qb-application")
    "qbctl-rs"       = @("qb-proto", "qb-ipc")
    "qbctld"         = @(
        "qb-domain",
        "qb-application",
        "qb-proto",
        "qb-ipc",
        "qb-metainfo",
        "qb-qbit",
        "qb-journal",
        "qb-win"
    )
}

$errors = New-Object System.Collections.Generic.List[string]

foreach ($entry in $allowed.GetEnumerator()) {
    $name = $entry.Key
    if (-not $workspace.ContainsKey($name)) {
        $errors.Add("expected workspace package '$name' is missing")
        continue
    }

    $permitted = @($entry.Value)
    $workspaceDeps = @(
        $workspace[$name].dependencies |
            Where-Object { $workspace.ContainsKey($_.name) } |
            ForEach-Object { $_.name }
    )

    foreach ($dependency in $workspaceDeps) {
        if ($dependency -notin $permitted) {
            $errors.Add("$name -> $dependency is a forbidden workspace dependency")
        }
    }
}

function Assert-SourceClean {
    param(
        [string]$Path,
        [string[]]$ForbiddenPatterns
    )

    $files = Get-ChildItem -Path $Path -Recurse -Filter *.rs -File
    foreach ($pattern in $ForbiddenPatterns) {
        $matches = $files | Select-String -Pattern $pattern
        foreach ($match in $matches) {
            $errors.Add("$($match.Path):$($match.LineNumber) violates architecture pattern '$pattern'")
        }
    }
}

Assert-SourceClean "crates/qb-domain/src" @(
    "qb_application",
    "qb_proto",
    "qb_ipc",
    "rusqlite",
    "reqwest",
    "windows::",
    "prost::",
    "clap::"
)

Assert-SourceClean "crates/qb-ipc/src" @(
    "qb_proto",
    "prost::"
)

Assert-SourceClean "crates/qb-application/src" @(
    "qb_proto",
    "qb_ipc",
    "qb_qbit",
    "qb_journal",
    "qb_win",
    "qb_metainfo",
    "rusqlite",
    "reqwest",
    "windows::",
    "prost::",
    "clap::"
)

Assert-SourceClean "apps/qbctld/src/server.rs" @(
    "qb_application",
    "qb_journal",
    "qb_proto",
    "qb_qbit",
    "qb_win",
    "qb_metainfo",
    "rusqlite",
    "reqwest"
)

if ($errors.Count -gt 0) {
    Write-Error ("Architecture invariant violations:`n - " + ($errors -join "`n - "))
}

Write-Host "Architecture dependency invariants: OK"
