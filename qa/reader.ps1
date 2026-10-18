param([Parameter(Mandatory=$true)][string]$Path,
      [Parameter(Mandatory=$true)][string]$AttemptDir)
$ErrorActionPreference = "Stop"
$full = [System.IO.Path]::GetFullPath($Path)
$root = [System.IO.Path]::GetFullPath($AttemptDir).TrimEnd('\') + '\'
if (-not $full.StartsWith($root, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Reader target escaped attemptDir"
}
# This is a real Windows handle: read/write sharing is allowed, delete sharing is not.
# The native task store still creates its temp file, but Windows can refuse replacement.
$handle = [System.IO.FileStream]::new(
    $full, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read,
    [System.IO.FileShare]::ReadWrite)
try {
    [Console]::Out.WriteLine("READER_READY $PID")
    [Console]::Out.Flush()
    $line = [Console]::In.ReadLine()
    if ($line -ne "RELEASE") { throw "Expected RELEASE event on stdin" }
} finally {
    $handle.Dispose()
}
[Console]::Out.WriteLine("READER_RELEASED $PID")
[Console]::Out.Flush()
