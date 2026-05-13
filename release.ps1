# release.ps1
# Automates version bumping, committing, and tagging for Glacier.Polaris

$csprojPath = "src/Glacier.Polaris/Glacier.Polaris.csproj"

if (-not (Test-Path $csprojPath)) {
    Write-Error "Could not find $csprojPath. Please run this script from the repository root."
    exit 1
}

# 1. Read the current version
[xml]$csproj = Get-Content $csprojPath
$currentVersionStr = $csproj.Project.PropertyGroup.Version
if (-not $currentVersionStr) {
    Write-Error "Could not find <Version> in $csprojPath"
    exit 1
}

Write-Host "Current version: $currentVersionStr"

# 2. Increment the version by 0.0.1
$version = [version]$currentVersionStr
$newVersion = [version]::new($version.Major, $version.Minor, ($version.Build + 1))
$newVersionStr = $newVersion.ToString()

Write-Host "New version: $newVersionStr"

# 3. Update the .csproj file
$csproj.Project.PropertyGroup.Version = $newVersionStr
$csproj.Save($csprojPath)

# 4. Git operations
Write-Host "Staging changes..."
git add -A

$commitMessage = Read-Host "Enter commit message"
if (-not $commitMessage) {
    $commitMessage = "Release v$newVersionStr"
}

Write-Host "Committing changes..."
git commit -m $commitMessage

Write-Host "Pushing to master..."
git push origin master

# 5. Tagging
$tagName = "v$newVersionStr"
Write-Host "Creating tag $tagName..."
git tag $tagName

Write-Host "Pushing tag $tagName..."
git push origin $tagName

Write-Host "Done! Release v$newVersionStr has been triggered."
