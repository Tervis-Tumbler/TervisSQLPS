function Remove-DataCollectionSetCacheFiles {
    param (
        $ComputerName
    )
    Invoke-Command -ComputerName $ComputerName -ScriptBlock {
        $CacheFiles = Get-ChildItem -Path $env:windir\Temp -File -Filter *.cache
        $CacheFiles | Remove-Item
    }
}