[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet(
        "archive-backup",
        "archive-restore",
        "archive-verify",
        "database-backup",
        "database-migrate",
        "database-restore",
        "database-status",
        "seed-demo"
    )]
    [string]$Operation,

    [Parameter(Position = 1)]
    [string]$Path,

    [Parameter(Position = 2)]
    [string]$Confirm
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
Set-Location -LiteralPath $RepositoryRoot
$ComposeProjectName = "makolet"
$AmbientComposeProjectName = [System.Environment]::GetEnvironmentVariable(
    "COMPOSE_PROJECT_NAME"
)
if ($null -ne $AmbientComposeProjectName) {
    if (
        $AmbientComposeProjectName -cnotmatch `
            "^makolet-smoke-[a-z0-9][a-z0-9_-]{0,40}-[0-9]{1,10}$" -or
        $env:MAKOLET_COMPOSE_ENV_FILE -cne ".env.example" -or
        $env:MAKOLET_ENVIRONMENT -cne "development" -or
        $env:POSTGRES_DB -cne "makolet_test_coverage"
    ) {
        throw "Refusing ambient Compose or Docker target selector: COMPOSE_PROJECT_NAME"
    }
    $ComposeProjectName = $AmbientComposeProjectName
    [System.Environment]::SetEnvironmentVariable("COMPOSE_PROJECT_NAME", $null, "Process")
}
$AmbientTargetSelectors = @(
    "COMPOSE_FILE",
    "COMPOSE_ENV_FILES",
    "COMPOSE_PATH_SEPARATOR",
    "COMPOSE_PROFILES",
    "COMPOSE_DISABLE_ENV_FILE",
    "DOCKER_CONFIG",
    "DOCKER_CONTEXT",
    "DOCKER_HOST"
)
foreach ($Selector in $AmbientTargetSelectors) {
    if ($null -ne [System.Environment]::GetEnvironmentVariable($Selector)) {
        throw "Refusing ambient Compose or Docker target selector: $Selector"
    }
}
$ComposeFileInput = Join-Path $RepositoryRoot "compose.yaml"
$ComposeFileItem = Get-Item -Force -LiteralPath $ComposeFileInput
if (
    -not ($ComposeFileItem -is [System.IO.FileInfo]) -or
    ($ComposeFileItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
) {
    throw "Repository compose.yaml must be a regular non-reparse file"
}
$ComposeFile = (Resolve-Path -LiteralPath $ComposeFileInput).Path
$ComposeArguments = @(
    "compose",
    "--file", $ComposeFile,
    "--project-directory", $RepositoryRoot,
    "--project-name", $ComposeProjectName
)
if ($env:MAKOLET_COMPOSE_ENV_FILE) {
    try {
        $ComposeEnvironmentFileItem = Get-Item `
            -Force `
            -LiteralPath $env:MAKOLET_COMPOSE_ENV_FILE
    }
    catch {
        throw "MAKOLET_COMPOSE_ENV_FILE must name a regular non-reparse file"
    }
    if (
        -not ($ComposeEnvironmentFileItem -is [System.IO.FileInfo]) -or
        ($ComposeEnvironmentFileItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
    ) {
        throw "MAKOLET_COMPOSE_ENV_FILE must name a regular non-reparse file"
    }
    $ComposeEnvironmentFile = (
        Resolve-Path -LiteralPath $env:MAKOLET_COMPOSE_ENV_FILE
    ).Path
    $ComposeArguments += @("--env-file", $ComposeEnvironmentFile)
}

function Invoke-Compose {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    & docker @ComposeArguments @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose operation failed"
    }
}

function Invoke-DockerWithWatchdog {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds,
        [switch]$CaptureOutput
    )
    $DockerCommand = @(Get-Command docker -CommandType Application -ErrorAction Stop)[0]
    $DockerExtension = [System.IO.Path]::GetExtension($DockerCommand.Source).ToLowerInvariant()
    if ($DockerExtension -in @(".cmd", ".bat")) {
        throw (
            "Refusing command-script Docker resolutions for watchdog-supervised " +
            "operations; install docker.exe"
        )
    }
    if (
        [System.Environment]::OSVersion.Platform -eq [System.PlatformID]::Win32NT -and
        $DockerExtension -cne ".exe"
    ) {
        throw "Watchdog-supervised Docker operations require docker.exe on Windows"
    }
    $ProcessStart = [System.Diagnostics.ProcessStartInfo]::new()
    $ProcessStart.FileName = $DockerCommand.Source
    $ProcessArguments = @($Arguments)
    $ProcessStart.UseShellExecute = $false
    $ProcessStart.CreateNoWindow = $true
    $ProcessStart.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
    $ProcessStart.RedirectStandardOutput = $true
    $ProcessStart.RedirectStandardError = $true
    $ArgumentListProperty = $ProcessStart.PSObject.Properties["ArgumentList"]
    if ($null -eq $ArgumentListProperty) {
        throw "A modern .NET runtime is required for bounded Docker Compose supervision"
    }
    foreach ($Argument in $ProcessArguments) {
        $ProcessStart.ArgumentList.Add($Argument)
    }
    $Process = [System.Diagnostics.Process]::new()
    $Process.StartInfo = $ProcessStart
    try {
        if (-not $Process.Start()) {
            throw "Docker Compose watchdog operation could not start"
        }
        $StandardOutputTask = $Process.StandardOutput.ReadToEndAsync()
        $StandardErrorTask = $Process.StandardError.ReadToEndAsync()
        $TimedOut = -not $Process.WaitForExit($TimeoutSeconds * 1000)
        if ($TimedOut) {
            try {
                $Process.Kill($true)
            }
            catch {
                $Process.Kill()
            }
            if (-not $Process.WaitForExit(5000)) {
                throw "Docker Compose watchdog cleanup exceeded its bounded deadline"
            }
        }
        $StandardOutput = $StandardOutputTask.GetAwaiter().GetResult()
        $StandardError = $StandardErrorTask.GetAwaiter().GetResult()
        if ($StandardOutput.Length -gt 0) {
            if ($CaptureOutput) {
                $StandardOutput
            }
            else {
                [Console]::Out.Write($StandardOutput)
            }
        }
        if ($StandardError.Length -gt 0) {
            [Console]::Error.Write($StandardError)
        }
        if ($TimedOut) {
            throw "Docker Compose operation exceeded its bounded watchdog"
        }
        if ($Process.ExitCode -ne 0) {
            throw "Docker Compose watchdog operation failed"
        }
    }
    finally {
        $Process.Dispose()
    }
}

function Invoke-ComposeWithWatchdog {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds,
        [switch]$CaptureOutput
    )
    Invoke-DockerWithWatchdog `
        -Arguments (@($ComposeArguments) + @($Arguments)) `
        -TimeoutSeconds $TimeoutSeconds `
        -CaptureOutput:$CaptureOutput
}

function Get-ArchiveTimeoutSeconds {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][double]$Default,
        [Parameter(Mandatory = $true)][double]$Maximum
    )
    $Value = [System.Environment]::GetEnvironmentVariable($Name)
    if ($null -eq $Value) {
        $Value = $Default.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    }
    $Parsed = 0.0
    $ParsedSuccessfully = [double]::TryParse(
        $Value,
        [System.Globalization.NumberStyles]::Float,
        [System.Globalization.CultureInfo]::InvariantCulture,
        [ref]$Parsed
    )
    if (
        -not $ParsedSuccessfully -or
        [double]::IsNaN($Parsed) -or
        [double]::IsInfinity($Parsed) -or
        $Parsed -le 0 -or
        $Parsed -gt $Maximum
    ) {
        throw "$Name must be positive and at most $Maximum"
    }
    return $Parsed
}

function Get-ArchiveWatchdogTimeouts {
    $OperationSeconds = Get-ArchiveTimeoutSeconds `
        -Name "MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS" `
        -Default 3600.0 `
        -Maximum 86400.0
    $CleanupSeconds = Get-ArchiveTimeoutSeconds `
        -Name "MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS" `
        -Default 30.0 `
        -Maximum 300.0
    return @{
        Operation = [int][Math]::Ceiling($OperationSeconds + $CleanupSeconds)
        Cleanup = [int][Math]::Ceiling($CleanupSeconds)
    }
}

function Remove-ArchiveOperationContainer {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )
    if (
        $ContainerName -cnotmatch `
            "^makolet-archive-(backup|verify|restore)-[0-9a-f]{32}$"
    ) {
        throw "Refusing to remove an unsafe archive operation container name"
    }
    $null = Invoke-DockerWithWatchdog `
        -Arguments @("container", "rm", "--force", $ContainerName) `
        -TimeoutSeconds $TimeoutSeconds `
        -CaptureOutput
}

function Get-ArchiveBackupLockName {
    if (
        $ComposeProjectName -cnotmatch `
            "^(makolet|makolet-smoke-[a-z0-9][a-z0-9_-]{0,40}-[0-9]{1,10})$"
    ) {
        throw "Archive backup cannot form a safe project lock name"
    }
    return "makolet_archive_backup_lock_$ComposeProjectName"
}

function Get-ArchiveBackupLockOwner {
    param(
        [Parameter(Mandatory = $true)][string]$LockName,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )
    if ($LockName -cnotmatch "^makolet_archive_backup_lock_[a-z0-9_-]{1,80}$") {
        throw "Refusing to inspect an unsafe archive backup lock"
    }
    return (
        Invoke-DockerWithWatchdog `
            -Arguments @(
                "volume", "inspect", "--format",
                '{{ index .Labels "com.makolet.archive-backup-lock.owner" }}',
                $LockName
            ) `
            -TimeoutSeconds $TimeoutSeconds `
            -CaptureOutput |
            Out-String
    ).Trim()
}

function Enter-ArchiveBackupLock {
    param(
        [Parameter(Mandatory = $true)][string]$LockName,
        [Parameter(Mandatory = $true)][string]$Owner,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )
    if ($Owner -cnotmatch "^makolet-archive-backup-[0-9a-f]{32}$") {
        throw "Refusing an unsafe archive backup lock owner"
    }
    $CreatedName = (
        Invoke-DockerWithWatchdog `
            -Arguments @(
                "volume", "create", "--label",
                "com.makolet.archive-backup-lock.owner=$Owner",
                $LockName
            ) `
            -TimeoutSeconds $TimeoutSeconds `
            -CaptureOutput |
            Out-String
    ).Trim()
    if ($CreatedName -cne $LockName) {
        throw "Archive backup lock creation returned an unexpected target"
    }
    if ((Get-ArchiveBackupLockOwner -LockName $LockName -TimeoutSeconds $TimeoutSeconds) -cne $Owner) {
        throw "Archive backup lock is already owned by another operation: $LockName"
    }
}

function Exit-ArchiveBackupLock {
    param(
        [Parameter(Mandatory = $true)][string]$LockName,
        [Parameter(Mandatory = $true)][string]$Owner,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )
    if ((Get-ArchiveBackupLockOwner -LockName $LockName -TimeoutSeconds $TimeoutSeconds) -cne $Owner) {
        throw "Archive backup lock ownership changed before release: $LockName"
    }
    $null = Invoke-DockerWithWatchdog `
        -Arguments @("volume", "rm", $LockName) `
        -TimeoutSeconds $TimeoutSeconds `
        -CaptureOutput
}

function Get-PostgresContainerId {
    $ContainerId = (Invoke-Compose -Arguments @("ps", "-q", "postgres") | Out-String).Trim()
    if ($ContainerId -notmatch "^[0-9a-f]{64}$") {
        throw "The PostgreSQL container is not running"
    }
    return $ContainerId
}

function Invoke-DatabaseBackupAuthentication {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("sign", "verify-copy")]
        [string]$Mode,
        [Parameter(Mandatory = $true)][string]$BackupPath,
        [Parameter(Mandatory = $true)][string]$AuthenticationPath,
        [string]$VerifiedCopyPath,
        [string]$CapacityLockDirectory
    )
    $Arguments = @(
        "run", "python", "-m", "makolet.interfaces.database_backup_auth",
        $Mode, $BackupPath, $AuthenticationPath
    )
    if ($Mode -eq "verify-copy") {
        if (-not $VerifiedCopyPath -or -not $CapacityLockDirectory) {
            throw "Database backup authentication verification target is missing"
        }
        $Arguments += @($VerifiedCopyPath, $CapacityLockDirectory)
    }
    & uv @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Database backup authentication operation failed"
    }
}

function Backup-Database {
    if (-not $Path) {
        throw "database-backup requires an output path"
    }
    $ParentInput = Split-Path -Parent $Path
    if (-not $ParentInput) {
        $ParentInput = "."
    }
    $null = New-Item -ItemType Directory -Force -Path $ParentInput
    $Parent = (Resolve-Path -LiteralPath $ParentInput).Path
    $Filename = Split-Path -Leaf $Path
    if (-not $Filename) {
        throw "database backup filename is empty"
    }
    $Destination = Join-Path $Parent $Filename
    $ChecksumDestination = "$Destination.sha256"
    $AuthenticationDestination = "$Destination.hmac-sha256"
    if (
        (Test-Path -LiteralPath $Destination) -or
        (Test-Path -LiteralPath $ChecksumDestination) -or
        (Test-Path -LiteralPath $AuthenticationDestination)
    ) {
        throw "Refusing to overwrite an existing database backup"
    }
    $Temporary = Join-Path $Parent (".makolet-database-{0}.tmp" -f [guid]::NewGuid().ToString("N"))
    $TemporaryChecksum = "$Temporary.sha256"
    $TemporaryAuthentication = "$Temporary.hmac-sha256"
    try {
        $CaptureArguments = @(
            "run", "python", "-m", "makolet.interfaces.database_backup_auth",
            "capture-command", $Temporary, $Parent, "--", "docker"
        )
        $CaptureArguments += $ComposeArguments
        $CaptureArguments += @(
            "exec", "-T", "postgres", "sh", "-eu", "-c",
            'exec pg_dump --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" --format=custom --compress=6 --no-owner --no-acl'
        )
        & uv @CaptureArguments
        if ($LASTEXITCODE -ne 0) {
            throw "Could not capture the bounded PostgreSQL backup"
        }
        $ValidationArguments = @(
            "run", "python", "-m", "makolet.interfaces.database_backup_auth",
            "validate-command", $Temporary, "--", "docker"
        )
        $ValidationArguments += $ComposeArguments
        $ValidationArguments += @("exec", "-T", "postgres", "pg_restore", "--list")
        & uv @ValidationArguments
        if ($LASTEXITCODE -ne 0) {
            throw "Could not validate the PostgreSQL backup"
        }
        $SidecarArguments = @(
            "run", "python", "-m", "makolet.interfaces.database_backup_auth",
            "write-sidecars", $Temporary, $TemporaryChecksum,
            $TemporaryAuthentication, $Filename, $Parent
        )
        $Digest = (& uv @SidecarArguments | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or $Digest -notmatch "^[0-9a-f]{64}$") {
            throw "Could not create the PostgreSQL backup sidecars"
        }
        Move-Item -LiteralPath $Temporary -Destination $Destination
        Move-Item -LiteralPath $TemporaryChecksum -Destination $ChecksumDestination
        Move-Item -LiteralPath $TemporaryAuthentication -Destination $AuthenticationDestination
        [pscustomobject]@{
            status = "backed_up"
            path = $Destination
            sha256 = $Digest
            authentication = "hmac-sha256-v1"
        } |
            ConvertTo-Json -Compress
    }
    finally {
        if (Test-Path -LiteralPath $Temporary) {
            Remove-Item -LiteralPath $Temporary -Force
        }
        if (Test-Path -LiteralPath $TemporaryChecksum) {
            Remove-Item -LiteralPath $TemporaryChecksum -Force
        }
        if (Test-Path -LiteralPath $TemporaryAuthentication) {
            Remove-Item -LiteralPath $TemporaryAuthentication -Force
        }
    }
}

function Restore-Database {
    if (-not $Path -or -not $Confirm) {
        throw "database-restore requires a backup path and exact database confirmation"
    }
    $Backup = [System.IO.Path]::GetFullPath($Path)
    if (-not (Test-Path -LiteralPath $Backup -PathType Leaf)) {
        throw "The database backup is missing"
    }
    $ChecksumPath = "$Backup.sha256"
    $AuthenticationPath = "$Backup.hmac-sha256"
    if (-not (Test-Path -LiteralPath $ChecksumPath -PathType Leaf)) {
        throw "The adjacent database backup checksum file is missing"
    }
    if (-not (Test-Path -LiteralPath $AuthenticationPath -PathType Leaf)) {
        throw "The adjacent database backup authentication file is missing"
    }
    $Expected = (& uv run python -m makolet.interfaces.database_backup_auth `
        read-checksum $ChecksumPath | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Database backup checksum validation failed"
    }
    if ($Expected -notmatch "^[0-9a-f]{64}$") {
        throw "Database backup checksum verification failed"
    }
    $VerificationRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
    $VerificationDirectory = Join-Path `
        $VerificationRoot `
        ("makolet-database-restore-{0}" -f [guid]::NewGuid().ToString("N"))
    $null = [System.IO.Directory]::CreateDirectory($VerificationDirectory)
    $VerifiedBackup = Join-Path $VerificationDirectory "authenticated.dump"
    try {
        Invoke-DatabaseBackupAuthentication `
            -Mode "verify-copy" `
            -BackupPath $Backup `
            -AuthenticationPath $AuthenticationPath `
            -VerifiedCopyPath $VerifiedBackup `
            -CapacityLockDirectory $VerificationRoot
        if ($Expected -ne (Get-Sha256 -LiteralPath $VerifiedBackup)) {
            throw "Database backup checksum verification failed"
        }
        $ContainerId = Get-PostgresContainerId
        $Remote = "/tmp/makolet-restore-{0}.dump" -f [guid]::NewGuid().ToString("N")
        $Database = (Invoke-Compose -Arguments @(
            "exec", "-T", "postgres", "sh", "-eu", "-c", 'printf %s "$POSTGRES_DB"'
        ) | Out-String).Trim()
        if ($Database -notmatch "^[A-Za-z][A-Za-z0-9_]{0,62}$") {
            throw "Configured database name is outside the restore script's safe identifier subset"
        }
        if ($Confirm -cne $Database) {
            throw "Confirmation does not exactly match the configured database: $Database"
        }
        & docker cp $VerifiedBackup "${ContainerId}:$Remote"
        if ($LASTEXITCODE -ne 0) {
            throw "Could not copy the backup into the PostgreSQL container"
        }
        $Suffix = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss") + "_" + $PID
        $Staging = "makolet_restore_$Suffix"
        $Previous = "makolet_previous_$Suffix"
        $ApiWasRunning = $false
        $WorkerWasRunning = $false
        $StagingCreated = $false
        $Swapped = $false
        try {
            Invoke-Compose -Arguments @(
                "exec", "-T", "-e", "RESTORE_DB=$Staging", "postgres", "sh", "-eu", "-c",
                'createdb --username="$POSTGRES_USER" "$RESTORE_DB"'
            ) | Out-Null
            $StagingCreated = $true
            Invoke-Compose -Arguments @(
                "exec", "-T", "-e", "RESTORE_DB=$Staging", "-e", "BACKUP_FILE=$Remote",
                "postgres", "sh", "-eu", "-c",
                'exec pg_restore --username="$POSTGRES_USER" --dbname="$RESTORE_DB" --exit-on-error --no-owner --no-acl "$BACKUP_FILE"'
            ) | Out-Null
            $Revision = (Invoke-Compose -Arguments @(
                "--progress", "quiet", "run", "--rm", "--no-deps",
                "-e", "MAKOLET_RESTORE_STAGING_DATABASE=$Staging",
                "migrate", "python", "-m", "makolet.interfaces.database_restore"
            ) | Out-String).Trim()
            if ($Revision -notmatch "^[A-Za-z0-9_-]+(,[A-Za-z0-9_-]+)*$") {
                throw "Staging migration returned an invalid revision"
            }
            $Running = @(Invoke-Compose -Arguments @("ps", "--status", "running", "--services"))
            $ApiWasRunning = $Running -contains "api"
            $WorkerWasRunning = $Running -contains "worker"
            & docker @ComposeArguments stop api worker *> $null
            $SwapSql = @"
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$Database' AND pid <> pg_backend_pid();
ALTER DATABASE "$Database" RENAME TO "$Previous";
ALTER DATABASE "$Staging" RENAME TO "$Database";
"@
            $SwapSql | & docker @ComposeArguments exec -T postgres sh -eu -c `
                'exec psql --username="$POSTGRES_USER" --dbname=postgres --no-psqlrc --single-transaction --set=ON_ERROR_STOP=1'
            if ($LASTEXITCODE -ne 0) {
                throw "Atomic database swap failed"
            }
            $Swapped = $true
            if ($ApiWasRunning) {
                Invoke-Compose -Arguments @("up", "-d", "api") | Out-Null
            }
            if ($WorkerWasRunning) {
                Invoke-Compose -Arguments @("up", "-d", "worker") | Out-Null
            }
            [pscustomobject]@{
                status = "restored"
                database = $Database
                previous_database = $Previous
                migration_revision = $Revision
            } | ConvertTo-Json -Compress
        }
        finally {
            & docker @ComposeArguments exec -T postgres rm -f -- $Remote 2>$null | Out-Null
            if (-not $Swapped -and $StagingCreated) {
                & docker @ComposeArguments @(
                    "exec", "-T", "-e", "RESTORE_DB=$Staging", "postgres", "sh", "-eu", "-c",
                    'dropdb --username="$POSTGRES_USER" --if-exists "$RESTORE_DB"'
                ) 2>$null | Out-Null
            }
            if ($ApiWasRunning) {
                & docker @ComposeArguments up -d api *> $null
            }
            if ($WorkerWasRunning) {
                & docker @ComposeArguments up -d worker *> $null
            }
        }
    }
    finally {
        if (Test-Path -LiteralPath $VerifiedBackup) {
            Remove-Item -LiteralPath $VerifiedBackup -Force
        }
        if (Test-Path -LiteralPath $VerificationDirectory) {
            Remove-Item -LiteralPath $VerificationDirectory -Force
        }
    }
}

function Invoke-ArchiveOperation {
    param([Parameter(Mandatory = $true)][string]$ArchiveOperation)
    if (-not $Path) {
        throw "$ArchiveOperation requires a backup directory"
    }
    if ($ArchiveOperation -eq "backup") {
        $null = New-Item -ItemType Directory -Force -Path $Path
    }
    $Destination = (Resolve-Path -LiteralPath $Path).Path
    if (-not $env:MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE) {
        throw "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE must name a regular non-reparse file"
    }
    try {
        $AuthenticationKey = (Resolve-Path -LiteralPath $env:MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE).Path
        $AuthenticationKeyItem = Get-Item -Force -LiteralPath $AuthenticationKey
    }
    catch {
        throw "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE must name a regular non-reparse file"
    }
    if (
        $AuthenticationKeyItem.PSIsContainer -or
        ($AuthenticationKeyItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
    ) {
        throw "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE must name a regular non-reparse file"
    }
    if ($AuthenticationKeyItem.Length -ne 32) {
        throw "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE must contain exactly 32 bytes"
    }
    $HostIsWindows = (
        [System.Environment]::OSVersion.Platform -eq [System.PlatformID]::Win32NT
    )
    $LinkTypeProperty = $AuthenticationKeyItem.PSObject.Properties["LinkType"]
    if (
        $HostIsWindows -and
        $null -ne $LinkTypeProperty -and
        $LinkTypeProperty.Value
    ) {
        throw "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE must not be a linked path"
    }
    $PathComparison = if ($HostIsWindows) {
        [System.StringComparison]::OrdinalIgnoreCase
    }
    else {
        [System.StringComparison]::Ordinal
    }
    $Separators = [char[]]@(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    $DestinationPrefix = $Destination.TrimEnd($Separators) + [System.IO.Path]::DirectorySeparatorChar
    if ($AuthenticationKey.StartsWith($DestinationPrefix, $PathComparison)) {
        throw "Archive backup authentication key must be outside the backup tree"
    }
    $WindowsKeyHandle = $null
    $ArchiveWorkerWasRunning = $false
    $ArchiveWorkerQuiescenceProven = $false
    $ArchiveWorkerWasRestarted = $false
    $ArchiveContainerCleanupRequired = $false
    $ArchiveContainerCleanupFailure = $null
    $ArchiveRecoveryFailure = $null
    $ArchiveBackupLockAcquired = $false
    $ArchiveWatchdogTimeouts = Get-ArchiveWatchdogTimeouts
    $ArchiveOperationWatchdogTimeoutSeconds = $ArchiveWatchdogTimeouts.Operation
    $ArchiveCleanupWatchdogTimeoutSeconds = $ArchiveWatchdogTimeouts.Cleanup
    $ArchiveContainerName = (
        "makolet-archive-$ArchiveOperation-" + [guid]::NewGuid().ToString("N")
    )
    if (
        $ArchiveContainerName -cnotmatch `
            "^makolet-archive-(backup|verify|restore)-[0-9a-f]{32}$"
    ) {
        throw "Generated archive operation container name is unsafe"
    }
    $ArchiveBackupLockName = Get-ArchiveBackupLockName
    try {
        $ContainerAuthenticationKey = "/run/secrets/makolet-archive-backup-auth.key"
        $AdditionalRunArguments = @()
        foreach ($VariableName in @(
            "MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES",
            "MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES",
            "MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES",
            "MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES",
            "MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS",
            "MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS",
            "MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS"
        )) {
            $VariableValue = [System.Environment]::GetEnvironmentVariable($VariableName)
            if ($null -ne $VariableValue) {
                $AdditionalRunArguments += @("--env", "${VariableName}=${VariableValue}")
            }
        }
        if ($HostIsWindows) {
            $WindowsKeyHandle = [System.IO.File]::Open(
                $AuthenticationKey,
                [System.IO.FileMode]::Open,
                [System.IO.FileAccess]::Read,
                [System.IO.FileShare]::Read
            )
            $KeyBuffer = [byte[]]::new(33)
            $KeyBytesRead = 0
            while ($KeyBytesRead -lt $KeyBuffer.Length) {
                $Read = $WindowsKeyHandle.Read(
                    $KeyBuffer,
                    $KeyBytesRead,
                    $KeyBuffer.Length - $KeyBytesRead
                )
                if ($Read -eq 0) {
                    break
                }
                $KeyBytesRead += $Read
            }
            if ($KeyBytesRead -ne 32) {
                throw "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE must contain exactly 32 bytes"
            }
            $ExactKey = [byte[]]::new(32)
            [System.Array]::Copy($KeyBuffer, $ExactKey, $ExactKey.Length)
            $Hasher = [System.Security.Cryptography.SHA256]::Create()
            try {
                $KeyDigestBytes = $Hasher.ComputeHash($ExactKey)
            }
            finally {
                $Hasher.Dispose()
            }
            $KeyDigest = [System.BitConverter]::ToString($KeyDigestBytes).
                Replace("-", "").ToLowerInvariant()
            $ContainerAuthenticationKey = "/run/secrets/makolet-archive-backup-auth.key.host"
            $AdditionalRunArguments += @(
                "--user", "10001:10001",
                "--env",
                "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_WINDOWS_BIND=windows-bind-staging-v1:$KeyDigest"
            )
        }
        else {
            $HostUserId = (& id -u | Out-String).Trim()
            if (
                $LASTEXITCODE -ne 0 -or
                $HostUserId -notmatch "^[0-9]{1,10}$" -or
                [long]$HostUserId -le 0 -or
                [long]$HostUserId -gt 2147483647
            ) {
                throw "Could not determine a safe non-root invoking POSIX user"
            }
            $HostGroupId = (& id -g | Out-String).Trim()
            if (
                $LASTEXITCODE -ne 0 -or
                $HostGroupId -notmatch "^[0-9]{1,10}$" -or
                [long]$HostGroupId -le 0 -or
                [long]$HostGroupId -gt 2147483647
            ) {
                throw "Could not determine a safe non-root invoking POSIX group"
            }
            $AdditionalRunArguments += @("--user", "${HostUserId}:${HostGroupId}")
        }
        $Volume = if ($ArchiveOperation -eq "backup") {
            "${Destination}:/backup"
        }
        else {
            "${Destination}:/backup:ro"
        }
        $KeyVolume = "${AuthenticationKey}:${ContainerAuthenticationKey}:ro"
        $Arguments = @(
            "--profile", "operations", "run", "--rm",
            "--name", $ArchiveContainerName,
            "--volume", $Volume,
            "--volume", $KeyVolume,
            "--env", "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE=$ContainerAuthenticationKey"
        )
        $Arguments += $AdditionalRunArguments
        if ($ArchiveOperation -eq "verify") {
            $Arguments += "--no-deps"
        }
        $Arguments += @("archive-tool", $ArchiveOperation, "/backup")
        if ($ArchiveOperation -eq "restore") {
            $ConfigurationOutput = Invoke-ComposeWithWatchdog `
                -Arguments @("--profile", "operations", "config", "--format", "json") `
                -TimeoutSeconds $ArchiveOperationWatchdogTimeoutSeconds `
                -CaptureOutput
            $Configuration = ($ConfigurationOutput | Out-String) | ConvertFrom-Json
            $Bucket = $Configuration.services.'archive-tool'.environment.MAKOLET_S3_BUCKET
            if ($Confirm -cne $Bucket) {
                throw "Confirmation does not exactly match the configured archive bucket: $Bucket"
            }
            $Arguments += @("--confirm-bucket", $Bucket)
        }
        if ($ArchiveOperation -eq "backup") {
            $WorkerRestartTimeoutValue = [System.Environment]::GetEnvironmentVariable(
                "MAKOLET_ARCHIVE_WORKER_RESTART_TIMEOUT_SECONDS"
            )
            if ($null -eq $WorkerRestartTimeoutValue) {
                $WorkerRestartTimeoutValue = "120"
            }
            if (
                $WorkerRestartTimeoutValue -cnotmatch "^[0-9]{1,4}$" -or
                [int]$WorkerRestartTimeoutValue -lt 1 -or
                [int]$WorkerRestartTimeoutValue -gt 3600
            ) {
                throw (
                    "MAKOLET_ARCHIVE_WORKER_RESTART_TIMEOUT_SECONDS must be " +
                    "an integer from 1 through 3600"
                )
            }
            $WorkerRestartTimeoutSeconds = [int]$WorkerRestartTimeoutValue
            Enter-ArchiveBackupLock `
                -LockName $ArchiveBackupLockName `
                -Owner $ArchiveContainerName `
                -TimeoutSeconds $WorkerRestartTimeoutSeconds
            $ArchiveBackupLockAcquired = $true
            $RunningServicesOutput = Invoke-ComposeWithWatchdog `
                -Arguments @(
                    "ps", "--status", "running", "--status", "restarting", "--services"
                ) `
                -TimeoutSeconds $WorkerRestartTimeoutSeconds `
                -CaptureOutput
            $RunningServices = @(
                ($RunningServicesOutput -split "\r?\n") |
                    Where-Object { $_.Length -gt 0 }
            )
            $ArchiveWorkerWasRunning = $RunningServices -contains "worker"
            Invoke-ComposeWithWatchdog `
                -Arguments @("stop", "worker") `
                -TimeoutSeconds $WorkerRestartTimeoutSeconds
            $RemainingServicesOutput = Invoke-ComposeWithWatchdog `
                -Arguments @(
                    "ps", "--status", "running", "--status", "restarting", "--services"
                ) `
                -TimeoutSeconds $WorkerRestartTimeoutSeconds `
                -CaptureOutput
            $RemainingServices = @(
                ($RemainingServicesOutput -split "\r?\n") |
                    Where-Object { $_.Length -gt 0 }
            )
            if ($RemainingServices -contains "worker") {
                throw "Worker did not reach a proven nonrunning state"
            }
            $ArchiveWorkerQuiescenceProven = $true
        }
        $ArchiveContainerCleanupRequired = $true
        Invoke-ComposeWithWatchdog `
            -Arguments $Arguments `
            -TimeoutSeconds $ArchiveOperationWatchdogTimeoutSeconds
        $ArchiveContainerCleanupRequired = $false
    }
    finally {
        if ($ArchiveContainerCleanupRequired) {
            try {
                Remove-ArchiveOperationContainer `
                    -ContainerName $ArchiveContainerName `
                    -TimeoutSeconds $ArchiveCleanupWatchdogTimeoutSeconds
            }
            catch {
                $ArchiveContainerCleanupFailure = [System.Exception]::new(
                    (
                        "Exact archive container $ArchiveContainerName cleanup failed " +
                        "or exceeded its bounded watchdog: $($_.Exception.Message)"
                    ),
                    $_.Exception
                )
            }
        }
        $ArchiveRecoverySafe = (
            $null -eq $ArchiveContainerCleanupFailure -and
            ($ArchiveOperation -ne "backup" -or $ArchiveWorkerQuiescenceProven)
        )
        if ($ArchiveOperation -eq "backup" -and $ArchiveBackupLockAcquired -and $ArchiveRecoverySafe) {
            try {
                if (
                    (Get-ArchiveBackupLockOwner `
                        -LockName $ArchiveBackupLockName `
                        -TimeoutSeconds $ArchiveCleanupWatchdogTimeoutSeconds) -cne
                    $ArchiveContainerName
                ) {
                    throw "Archive backup lock ownership changed"
                }
            }
            catch {
                $ArchiveRecoveryFailure = $_
                $ArchiveRecoverySafe = $false
            }
        }
        if ($ArchiveWorkerWasRunning -and $ArchiveRecoverySafe) {
            try {
                Invoke-ComposeWithWatchdog `
                    -Arguments @("up", "-d", "--wait", "worker") `
                    -TimeoutSeconds $WorkerRestartTimeoutSeconds
                $ArchiveWorkerWasRunning = $false
                $ArchiveWorkerWasRestarted = $true
            }
            catch {
                $ArchiveRecoveryFailure = $_
                $ArchiveRecoverySafe = $false
                try {
                    Invoke-ComposeWithWatchdog `
                        -Arguments @("stop", "worker") `
                        -TimeoutSeconds $WorkerRestartTimeoutSeconds
                    $ArchiveWorkerWasRunning = $false
                    [Console]::Error.WriteLine(
                        "Worker stopped after archive backup restart failure"
                    )
                }
                catch {
                    [Console]::Error.WriteLine(
                        "Worker state is unproven after archive backup restart failure"
                    )
                }
            }
        }
        if ($ArchiveOperation -eq "backup" -and $ArchiveBackupLockAcquired -and $ArchiveRecoverySafe) {
            try {
                Exit-ArchiveBackupLock `
                    -LockName $ArchiveBackupLockName `
                    -Owner $ArchiveContainerName `
                    -TimeoutSeconds $ArchiveCleanupWatchdogTimeoutSeconds
                $ArchiveBackupLockAcquired = $false
            }
            catch {
                $ArchiveRecoveryFailure = $_
                $ArchiveRecoverySafe = $false
                if ($ArchiveWorkerWasRestarted) {
                    try {
                        Invoke-ComposeWithWatchdog `
                            -Arguments @("stop", "worker") `
                            -TimeoutSeconds $WorkerRestartTimeoutSeconds
                        [Console]::Error.WriteLine(
                            "Worker stopped after archive backup lock release failure"
                        )
                    }
                    catch {
                        [Console]::Error.WriteLine(
                            "Worker state is unproven after archive backup lock release failure"
                        )
                    }
                }
            }
        }
        if ($ArchiveWorkerWasRunning -and -not $ArchiveRecoverySafe) {
            [Console]::Error.WriteLine(
                "Worker restart suppressed or state unproven; preserve lock " +
                "$ArchiveBackupLockName until recovery"
            )
        }
        if ($ArchiveBackupLockAcquired) {
            [Console]::Error.WriteLine(
                "Archive backup lock intentionally retained for recovery: " +
                $ArchiveBackupLockName
            )
        }
        if ($null -ne $WindowsKeyHandle) {
            $WindowsKeyHandle.Dispose()
        }
        if ($null -ne $ArchiveContainerCleanupFailure) {
            throw $ArchiveContainerCleanupFailure
        }
        if ($null -ne $ArchiveRecoveryFailure) {
            throw $ArchiveRecoveryFailure
        }
    }
}

switch ($Operation) {
    "archive-backup" { Invoke-ArchiveOperation -ArchiveOperation "backup" }
    "archive-restore" { Invoke-ArchiveOperation -ArchiveOperation "restore" }
    "archive-verify" { Invoke-ArchiveOperation -ArchiveOperation "verify" }
    "database-backup" { Backup-Database }
    "database-migrate" { Invoke-Compose -Arguments @("run", "--rm", "migrate") }
    "database-restore" { Restore-Database }
    "database-status" {
        Invoke-Compose -Arguments @(
            "run", "--rm", "--no-deps", "migrate", "makolet", "database", "status", "--json"
        )
    }
    "seed-demo" {
        Invoke-Compose -Arguments @("--profile", "demo", "run", "--rm", "demo-seed")
    }
}
