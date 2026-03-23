param(
    [switch]$DryRun,
    [switch]$NoCache,
    [switch]$UseLastCommitIfClean = $true
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$serverIP = "13.235.56.166"
$sshKey = Join-Path $PSScriptRoot "ssh_key.pem"
$remoteUser = "ubuntu"
$remotePath = "/home/ubuntu/live-dashboard"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Write-Section {
    param([string]$Text)
    Write-Host ""
    Write-Host "== $Text ==" -ForegroundColor Cyan
}

function Run-Command {
    param([string]$CommandText)
    if ($DryRun) {
        Write-Host "[dry-run] $CommandText" -ForegroundColor Yellow
        return
    }
    Invoke-Expression $CommandText
}

function Get-Lines {
    param([string]$Cmd)
    $out = & powershell -NoProfile -Command $Cmd
    if ($null -eq $out) {
        if ($LASTEXITCODE -ne 0) {
            return @()
        }
        return @()
    }
    $lines = @($out) | ForEach-Object { "$_".Trim() } | Where-Object { $_ -ne "" }
    if ($lines.Count -eq 0 -and $LASTEXITCODE -ne 0) {
        return @()
    }
    return $lines
}

function Unique-Array {
    param([string[]]$Items)
    $set = New-Object "System.Collections.Generic.HashSet[string]" ([System.StringComparer]::OrdinalIgnoreCase)
    $out = New-Object "System.Collections.Generic.List[string]"
    foreach ($item in $Items) {
        if (-not $item) { continue }
        if ($set.Add($item)) {
            $out.Add($item) | Out-Null
        }
    }
    return $out.ToArray()
}

function Is-RelevantPath {
    param([string]$PathText)
    $normalized = ($PathText -replace "\\","/").Trim()
    if (-not $normalized) { return $false }

    $skipPrefixes = @(
        ".git/",
        ".vscode/",
        "backend/.venv/",
        "frontend/my-app/node_modules/",
        "frontend/my-app/.next/",
        "deploy/logs/"
    )
    foreach ($prefix in $skipPrefixes) {
        if ($normalized.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            return $false
        }
    }

    return (
        $normalized.StartsWith("backend/") -or
        $normalized.StartsWith("frontend/") -or
        $normalized -eq "deploy/docker-compose.prod.yml"
    )
}

if (-not (Test-Path ".git")) {
    throw "Run this script from inside the project repository."
}

if (-not (Test-Path $sshKey)) {
    throw "SSH key not found at $sshKey"
}

$uploadCandidates = @()
$deleteCandidates = @()

# Unstaged + staged + untracked changes
$uploadCandidates += Get-Lines 'git diff --name-only --diff-filter=ACMR 2>$null'
$uploadCandidates += Get-Lines 'git diff --name-only --cached --diff-filter=ACMR 2>$null'
$uploadCandidates += Get-Lines 'git ls-files --others --exclude-standard 2>$null'
$deleteCandidates += Get-Lines 'git diff --name-only --diff-filter=D 2>$null'
$deleteCandidates += Get-Lines 'git diff --name-only --cached --diff-filter=D 2>$null'

$uploadCandidates = Unique-Array $uploadCandidates
$deleteCandidates = Unique-Array $deleteCandidates

$changeSource = "working_tree"
if (
    $UseLastCommitIfClean -and
    $uploadCandidates.Count -eq 0 -and
    $deleteCandidates.Count -eq 0
) {
    $hasPrevCommit = $true
    & git rev-parse --verify HEAD~1 *> $null
    if ($LASTEXITCODE -ne 0) {
        $hasPrevCommit = $false
    }
    if ($hasPrevCommit) {
        $uploadCandidates = Unique-Array (Get-Lines 'git diff --name-only --diff-filter=ACMR HEAD~1 HEAD 2>$null')
        $deleteCandidates = Unique-Array (Get-Lines 'git diff --name-only --diff-filter=D HEAD~1 HEAD 2>$null')
        $changeSource = "last_commit"
    }
}

$uploadFiles = @($uploadCandidates | Where-Object { Is-RelevantPath $_ })
$deleteFiles = @($deleteCandidates | Where-Object { Is-RelevantPath $_ })

if ($uploadFiles.Count -eq 0 -and $deleteFiles.Count -eq 0) {
    Write-Host "No deploy-relevant changes found (backend/frontend/docker-compose)." -ForegroundColor Yellow
    exit 0
}

$backendChanged = (
    @($uploadFiles | Where-Object { $_.StartsWith("backend/") }).Count -gt 0 -or
    @($deleteFiles | Where-Object { $_.StartsWith("backend/") }).Count -gt 0
)
$frontendChanged = (
    @($uploadFiles | Where-Object { $_.StartsWith("frontend/") }).Count -gt 0 -or
    @($deleteFiles | Where-Object { $_.StartsWith("frontend/") }).Count -gt 0
)
$composeChanged = ($uploadFiles -contains "deploy/docker-compose.prod.yml")

Write-Section "Fast Deploy"
Write-Host "Server: $serverIP"
Write-Host "Change source: $changeSource"
Write-Host "Upload files: $($uploadFiles.Count)"
Write-Host "Delete files: $($deleteFiles.Count)"
Write-Host "Services affected: backend=$backendChanged frontend=$frontendChanged compose=$composeChanged"

Write-Section "Sync Files"
Run-Command "ssh -i `"$sshKey`" -o StrictHostKeyChecking=no ${remoteUser}@${serverIP} `"mkdir -p $remotePath`""

foreach ($path in $uploadFiles) {
    if (-not (Test-Path $path)) {
        continue
    }

    $remoteTarget = if ($path -eq "deploy/docker-compose.prod.yml") {
        "$remotePath/docker-compose.yml"
    } else {
        "$remotePath/$($path -replace '\\','/')"
    }
    $remoteDir = [System.IO.Path]::GetDirectoryName($remoteTarget).Replace('\', '/')

    Run-Command "ssh -i `"$sshKey`" -o StrictHostKeyChecking=no ${remoteUser}@${serverIP} `"mkdir -p '$remoteDir'`""
    Run-Command "scp -i `"$sshKey`" -o StrictHostKeyChecking=no `"$path`" ${remoteUser}@${serverIP}:`"$remoteTarget`""
}

foreach ($path in $deleteFiles) {
    if (-not ($path.StartsWith("backend/") -or $path.StartsWith("frontend/"))) {
        continue
    }
    $remoteTarget = "$remotePath/$($path -replace '\\','/')"
    Run-Command "ssh -i `"$sshKey`" -o StrictHostKeyChecking=no ${remoteUser}@${serverIP} `"rm -f '$remoteTarget'`""
}

Write-Section "Rebuild/Restart"
$buildTargets = @()
if ($backendChanged) { $buildTargets += "backend" }
if ($frontendChanged) { $buildTargets += "frontend" }

$buildPart = ""
if ($buildTargets.Count -gt 0) {
    $noCachePart = if ($NoCache) { "--no-cache " } else { "" }
    $buildPart = "docker compose build ${noCachePart}$($buildTargets -join ' ') && "
}

$upTargets = if ($buildTargets.Count -gt 0) { " $($buildTargets -join ' ')" } else { "" }
$remoteDeployCmd = "cd $remotePath && ${buildPart}docker compose up -d --remove-orphans$upTargets && docker compose ps"
Run-Command "ssh -i `"$sshKey`" -o StrictHostKeyChecking=no ${remoteUser}@${serverIP} `"$remoteDeployCmd`""

Write-Section "Quick Checks"
Run-Command "ssh -i `"$sshKey`" -o StrictHostKeyChecking=no ${remoteUser}@${serverIP} `"curl -fsS http://localhost:8000/health`""
Run-Command "ssh -i `"$sshKey`" -o StrictHostKeyChecking=no ${remoteUser}@${serverIP} `"curl -sSI http://localhost:3000 | head -n 1`""

Write-Host ""
Write-Host "Fast deployment complete." -ForegroundColor Green
