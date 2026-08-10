<#
.SYNOPSIS
    Detects the current project version and updates it in matching text files.

.DESCRIPTION
    The script performs the following operations:

    1. Recursively searches for supported project files:
       - pyproject.toml
       - pytoml
       - pom.xml

    2. Detects the current project version.

    3. Calculates the next semantic version:
       - Bugfix/Patch: 1.2.3 -> 1.2.4
       - Minor:        1.2.3 -> 1.3.0
       - Major:        1.2.3 -> 2.0.0

       The default increment is Bugfix.

    4. Searches recursively for the exact, case-sensitive current-version
       string in eligible text files.

    5. Displays the files that will be modified and the number of occurrences.

    6. Prompts the user to enter y or n.

    7. Replaces the current-version string with the new version.

    Excluded directories are not traversed. The default exclusions include:

       - target
       - .git
       - .hg
       - .svn
       - .idea
       - .vscode
       - .venv
       - venv
       - node_modules
       - build
       - dist
       - __pycache__

    Common binary file extensions are also excluded.

.PARAMETER Bump
    Specifies the semantic-version increment.

    Accepted values:
       - Bugfix
       - Patch
       - Minor
       - Major

    The default value is Bugfix.

.PARAMETER Path
    Specifies the directory from which the recursive search starts.

    The default value is the current directory.

.PARAMETER Help
    Displays detailed help and exits.

.PARAMETER WhatIf
    Displays the version and files that would be modified without prompting
    for confirmation and without modifying any file.

.EXAMPLE
    .\Upgrade-Version.ps1

    Performs the default Bugfix increment:

       1.2.3 -> 1.2.4

.EXAMPLE
    .\Upgrade-Version.ps1 -Bump Patch

    Performs a Patch increment, equivalent to Bugfix:

       1.2.3 -> 1.2.4

.EXAMPLE
    .\Upgrade-Version.ps1 -Bump Minor

    Performs a Minor increment:

       1.2.3 -> 1.3.0

.EXAMPLE
    .\Upgrade-Version.ps1 -Bump Major

    Performs a Major increment:

       1.2.3 -> 2.0.0

.EXAMPLE
    .\Upgrade-Version.ps1 -Path C:\Projects\MyProject

    Searches under the specified directory.

.EXAMPLE
    .\Upgrade-Version.ps1 -Bump Minor -WhatIf

    Displays the files that would be modified without changing them.

.EXAMPLE
    .\Upgrade-Version.ps1 -Help

    Displays detailed help.

.EXAMPLE
    .\Upgrade-Version.ps1 -h

    Displays detailed help using the short alias.

.EXAMPLE
    Get-Help .\Upgrade-Version.ps1 -Full

    Displays full PowerShell help for the script.
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Position = 0)]
    [ValidateSet("Bugfix", "Patch", "Minor", "Major")]
    [string]$Bump = "Bugfix",

    [Parameter()]
    [ValidateScript({
        if (-not (Test-Path -LiteralPath $_ -PathType Container)) {
            throw "Directory not found: '$_'."
        }

        return $true
    })]
    [string]$Path = (Get-Location).Path,

    [Parameter()]
    [Alias("h")]
    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Help) {
    Get-Help -Name $PSCommandPath -Detailed
    return
}

# =============================================================================
# Configuration
# =============================================================================

$excludedDirectoryNames = @(
    "target",
    ".git",
    ".hg",
    ".svn",
    ".idea",
    ".vscode",
    ".venv",
    "venv",
    "node_modules",
    "build",
    "dist",
    "__pycache__"
)

$excludedExtensions = @(
    ".7z",
    ".avi",
    ".bmp",
    ".class",
    ".dll",
    ".doc",
    ".docx",
    ".eot",
    ".exe",
    ".gif",
    ".gz",
    ".ico",
    ".jar",
    ".jpeg",
    ".jpg",
    ".mov",
    ".mp3",
    ".mp4",
    ".o",
    ".obj",
    ".otf",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".pyc",
    ".so",
    ".tar",
    ".tif",
    ".tiff",
    ".ttf",
    ".war",
    ".webp",
    ".woff",
    ".woff2",
    ".xls",
    ".xlsx",
    ".zip"
)

# =============================================================================
# File discovery
# =============================================================================

function Get-EligibleFiles {
    <#
        Recursively returns files while avoiding excluded directories.

        Excluded directories are not traversed at all.
    #>

    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$RootPath
    )

    $items = Get-ChildItem `
        -LiteralPath $RootPath `
        -Force `
        -ErrorAction SilentlyContinue

    foreach ($item in $items) {
        if ($item.PSIsContainer) {
            if ($excludedDirectoryNames -notcontains $item.Name) {
                Get-EligibleFiles -RootPath $item.FullName
            }
            else {
                Write-Verbose "Excluded directory: '$($item.FullName)'."
            }
        }
        else {
            Write-Output $item
        }
    }
}

function Get-RelativeFilePath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath,

        [Parameter(Mandatory)]
        [string]$RootPath
    )

    $relativePath = $FilePath.Substring($RootPath.Length)
    $relativePath = $relativePath -replace '^[\\/]+', ""

    return $relativePath
}

# =============================================================================
# Text-file handling
# =============================================================================

function Get-TextFileInfo {
    <#
        Reads a text file while retaining the detected encoding.

        StreamReader automatically detects UTF-8, UTF-16 and UTF-32 byte-order
        marks. Files containing NUL characters are considered binary.
    #>

    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath
    )

    $reader = $null

    try {
        $reader = New-Object System.IO.StreamReader(
            $FilePath,
            $true
        )

        $content = $reader.ReadToEnd()
        $encoding = $reader.CurrentEncoding
    }
    catch {
        throw "Unable to read the file as text: $($_.Exception.Message)"
    }
    finally {
        if ($null -ne $reader) {
            $reader.Dispose()
        }
    }

    if ($content.Contains([char]0)) {
        throw "The file appears to be binary."
    }

    return [PSCustomObject]@{
        Path     = $FilePath
        Content  = $content
        Encoding = $encoding
    }
}

function Set-TextFileContent {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath,

        [Parameter(Mandatory)]
        [AllowEmptyString()]
        [string]$Content,

        [Parameter(Mandatory)]
        [System.Text.Encoding]$Encoding
    )

    # Use UTF-8 without BOM for UTF-8 files.
    # Preserve the original encoding for UTF-16 and UTF-32 files.
    if ($Encoding.WebName -eq "utf-8") {
        $outputEncoding = New-Object System.Text.UTF8Encoding(
            $false,
            $true
        )
    }
    else {
        $outputEncoding = $Encoding
    }

    $contentBytes = $outputEncoding.GetBytes($Content)

    [System.IO.File]::WriteAllBytes(
        $FilePath,
        $contentBytes
    )
}

function Get-StringOccurrenceCount {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [AllowEmptyString()]
        [string]$Content,

        [Parameter(Mandatory)]
        [string]$SearchValue
    )

    if ($SearchValue.Length -eq 0) {
        return 0
    }

    $count = 0
    $startIndex = 0

    while ($startIndex -lt $Content.Length) {
        $foundIndex = $Content.IndexOf(
            $SearchValue,
            $startIndex
        )

        if ($foundIndex -lt 0) {
            break
        }

        $count++
        $startIndex = $foundIndex + $SearchValue.Length
    }

    return $count
}

# =============================================================================
# Version handling
# =============================================================================

function Get-NextVersion {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Version,

        [Parameter(Mandatory)]
        [ValidateSet("Bugfix", "Patch", "Minor", "Major")]
        [string]$Increment
    )

    $versionPattern = (
        '^(?<major>\d+)\.' +
        '(?<minor>\d+)' +
        '(?:\.(?<patch>\d+))?' +
        '(?:[-+].*)?$'
    )

    if ($Version -notmatch $versionPattern) {
        throw (
            "Unsupported version format: '$Version'. " +
            "Expected a semantic version such as 1.2.3."
        )
    }

    [int]$major = $Matches["major"]
    [int]$minor = $Matches["minor"]
    [int]$patch = 0

    if (
        $null -ne $Matches["patch"] -and
        $Matches["patch"].Length -gt 0
    ) {
        $patch = [int]$Matches["patch"]
    }

    switch ($Increment.ToLowerInvariant()) {
        "bugfix" {
            $patch++
        }

        "patch" {
            $patch++
        }

        "minor" {
            $minor++
            $patch = 0
        }

        "major" {
            $major++
            $minor = 0
            $patch = 0
        }

        default {
            throw "Unsupported version increment: '$Increment'."
        }
    }

    return "$major.$minor.$patch"
}

function Get-TomlProjectVersion {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath
    )

    $fileInfo = Get-TextFileInfo -FilePath $FilePath
    $content = $fileInfo.Content

    $sectionPattern = (
        '(?ms)' +
        '^\s*\[(?:project|tool\.poetry)\]\s*(?:\r?\n|$)' +
        '(?<section>.*?)' +
        '(?=^\s*\[|\z)'
    )

    $sectionMatch = :Match(
        $content,
        $sectionPattern
    )

    if (-not $sectionMatch.Success) {
        return $null
    }

    $versionPattern = (
        '(?m)' +
        '^\s*version\s*=\s*["'']' +
        '(?<version>[^"'']+)' +
        '["'']\s*(?:#.*)?$'
    )

    $versionMatch = :Match(
        $sectionMatch.Groups["section"].Value,
        $versionPattern
    )

    if (-not $versionMatch.Success) {
        return $null
    }

    return $versionMatch.Groups["version"].Value.Trim()
}

function Get-MavenProjectVersion {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath
    )

    $fileInfo = Get-TextFileInfo -FilePath $FilePath

    try {
        [xml]$pom = $fileInfo.Content
    }
    catch {
        throw "Invalid Maven XML: $($_.Exception.Message)"
    }

    if ($null -eq $pom.DocumentElement) {
        return $null
    }

    $namespaceUri = $pom.DocumentElement.NamespaceURI
    $versionNode = $null

    if (
        $null -eq $namespaceUri -or
        $namespaceUri.Trim().Length -eq 0
    ) {
        $versionNode = $pom.SelectSingleNode("/project/version")
    }
    else {
        $namespaceManager = New-Object System.Xml.XmlNamespaceManager(
            $pom.NameTable
        )

        $namespaceManager.AddNamespace(
            "m",
            $namespaceUri
        )

        $versionNode = $pom.SelectSingleNode(
            "/m:project/m:version",
            $namespaceManager
        )
    }

    if ($null -eq $versionNode) {
        # Maven modules may inherit their version from a parent POM.
        return $null
    }

    return $versionNode.InnerText.Trim()
}

# =============================================================================
# Confirmation
# =============================================================================

function Confirm-VersionUpdate {
    [CmdletBinding()]
    param()

    while ($true) {
        $answer = Read-Host "Apply these changes? [y/n]"

        if ($null -eq $answer) {
            continue
        }

        $normalizedAnswer = $answer.Trim().ToLowerInvariant()

        switch ($normalizedAnswer) {
            "y" {
                return $true
            }

            "yes" {
                return $true
            }

            "n" {
                return $false
            }

            "no" {
                return $false
            }

            default {
                Write-Host `
                    "Invalid answer. Enter y or n." `
                    -ForegroundColor Yellow
            }
        }
    }
}

# =============================================================================
# Main execution
# =============================================================================

$resolvedPath = (Resolve-Path -LiteralPath $Path).Path

# Remove a trailing slash, except for a filesystem root.
if (
    $resolvedPath.Length -gt 3 -and
    ($resolvedPath.EndsWith("\") -or $resolvedPath.EndsWith("/"))
) {
    $resolvedPath = $resolvedPath.TrimEnd("\", "/")
}

Write-Host ""
Write-Host "Version Upgrade" -ForegroundColor Cyan
Write-Host "===============" -ForegroundColor Cyan
Write-Host "Search path: $resolvedPath"
Write-Host ""

# -----------------------------------------------------------------------------
# Discover eligible files
# -----------------------------------------------------------------------------

Write-Host "Discovering files..." -ForegroundColor Cyan

$allFiles = @(
    Get-EligibleFiles -RootPath $resolvedPath
)

if ($allFiles.Count -eq 0) {
    throw "No eligible files were found under '$resolvedPath'."
}

$projectFiles = @(
    $allFiles |
    Where-Object {
        $_.Name -in @(
            "pyproject.toml",
            "pom.xml"
        )
    }
)

if ($projectFiles.Count -eq 0) {
    throw (
        "No pyproject.toml, pytoml, or pom.xml file was found under " +
        "'$resolvedPath'."
    )
}

# -----------------------------------------------------------------------------
# Detect the current version
# -----------------------------------------------------------------------------

$detectedProjects = @()

foreach ($file in $projectFiles) {
    try {
        $projectType = $null
        $version = $null

        if ($file.Name -eq "pom.xml") {
            $projectType = "Maven"
            $version = Get-MavenProjectVersion -FilePath $file.FullName
        }
        else {
            $projectType = "Python"
            $version = Get-TomlProjectVersion -FilePath $file.FullName
        }

        if (
            $null -ne $version -and
            $version.Trim().Length -gt 0
        ) {
            $detectedProjects += [PSCustomObject]@{
                Type    = $projectType
                Version = $version
                File    = $file.FullName
            }
        }
        else {
            Write-Verbose (
                "No direct project version found in " +
                "'$($file.FullName)'."
            )
        }
    }
    catch {
        throw (
            "Unable to read the project version from " +
            "'$($file.FullName)': $($_.Exception.Message)"
        )
    }
}

if ($detectedProjects.Count -eq 0) {
    throw (
        "Supported project files were found, but no direct project " +
        "version could be detected."
    )
}

$distinctVersions = @(
    $detectedProjects |
    Select-Object -ExpandProperty Version |
    Sort-Object -Unique
)

if ($distinctVersions.Count -gt 1) {
    Write-Host ""
    Write-Host "Different project versions were found:" `
        -ForegroundColor Yellow
    Write-Host ""

    $detectedProjects |
        Sort-Object File |
        Format-Table Type, Version, File -AutoSize |
        Out-Host

    throw (
        "A single reference version could not be selected. " +
        "Align the project versions or run the script from a more " +
        "specific directory."
    )
}

$currentVersion = $distinctVersions[0]

$nextVersion = Get-NextVersion `
    -Version $currentVersion `
    -Increment $Bump

Write-Host "Current version : $currentVersion"
Write-Host "Increment type  : $Bump"
Write-Host "Next version    : $nextVersion" -ForegroundColor Green
Write-Host ""

# -----------------------------------------------------------------------------
# Search for the current version
# -----------------------------------------------------------------------------

Write-Host (
    "Searching for the exact string '$currentVersion'..."
) -ForegroundColor Cyan

$filesToUpdate = @()

foreach ($file in $allFiles) {
    $extension = $file.Extension.ToLowerInvariant()

    if ($excludedExtensions -contains $extension) {
        Write-Verbose (
            "Excluded file extension skipped: '$($file.FullName)'."
        )
        continue
    }

    try {
        $textFile = Get-TextFileInfo -FilePath $file.FullName

        $occurrenceCount = Get-StringOccurrenceCount `
            -Content $textFile.Content `
            -SearchValue $currentVersion

        if ($occurrenceCount -gt 0) {
            $relativePath = Get-RelativeFilePath `
                -FilePath $file.FullName `
                -RootPath $resolvedPath

            $filesToUpdate += [PSCustomObject]@{
                File        = $file.FullName
                Relative    = $relativePath
                Occurrences = $occurrenceCount
                TextFile    = $textFile
            }
        }
    }
    catch {
        Write-Verbose (
            "File skipped: '$($file.FullName)'. " +
            "Reason: $($_.Exception.Message)"
        )
    }
}

if ($filesToUpdate.Count -eq 0) {
    throw (
        "The current version '$currentVersion' was not found in any " +
        "eligible text file."
    )
}

$totalOccurrences = (
    $filesToUpdate |
    Measure-Object -Property Occurrences -Sum
).Sum

# -----------------------------------------------------------------------------
# Display proposed changes
# -----------------------------------------------------------------------------

Write-Host ""
Write-Host "Files that will be modified:" -ForegroundColor Yellow
Write-Host ""

$filesToUpdate |
    Sort-Object Relative |
    Select-Object `
        @{
            Name = "Occurrences"
            Expression = { $_.Occurrences }
        },
        @{
            Name = "File"
            Expression = { $_.Relative }
        } |
    Format-Table -AutoSize |
    Out-Host

Write-Host "Total files       : $($filesToUpdate.Count)"
Write-Host "Total replacements: $totalOccurrences"
Write-Host ""
Write-Host "$currentVersion -> $nextVersion" -ForegroundColor Green
Write-Host ""

# -----------------------------------------------------------------------------
# WhatIf mode
# -----------------------------------------------------------------------------

if ($WhatIfPreference) {
    Write-Host "WhatIf mode: no files were modified." `
        -ForegroundColor Yellow

    return
}

# -----------------------------------------------------------------------------
# Interactive confirmation
# -----------------------------------------------------------------------------

if (-not (Confirm-VersionUpdate)) {
    Write-Host ""
    Write-Host "Operation cancelled. No files were modified." `
        -ForegroundColor Yellow

    return
}

# -----------------------------------------------------------------------------
# Update matching files
# -----------------------------------------------------------------------------

Write-Host ""
Write-Host "Updating files..." -ForegroundColor Cyan

$updatedFiles = 0
$appliedReplacements = 0

foreach ($item in $filesToUpdate) {
    try {
        # String.Replace performs a literal, case-sensitive replacement.
        $updatedContent = $item.TextFile.Content.Replace(
            $currentVersion,
            $nextVersion
        )

        $description = (
            "Replace '$currentVersion' with '$nextVersion' in " +
            "'$($item.Relative)'"
        )

        if ($PSCmdlet.ShouldProcess($item.File, $description)) {
            Set-TextFileContent `
                -FilePath $item.File `
                -Content $updatedContent `
                -Encoding $item.TextFile.Encoding

            $updatedFiles++
            $appliedReplacements += $item.Occurrences

            Write-Host "Updated: $($item.Relative)" `
                -ForegroundColor Green
        }
    }
    catch {
        throw (
            "Unable to update '$($item.File)': " +
            $_.Exception.Message
        )
    }
}

# -----------------------------------------------------------------------------
# Final summary
# -----------------------------------------------------------------------------

Write-Host ""
Write-Host "Version upgrade completed." -ForegroundColor Green
Write-Host "Version      : $currentVersion -> $nextVersion"
Write-Host "Files updated: $updatedFiles"
Write-Host "Replacements : $appliedReplacements"