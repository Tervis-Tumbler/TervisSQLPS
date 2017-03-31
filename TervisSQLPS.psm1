function Remove-DataCollectionSetCacheFiles {
    param (
        $ComputerName
    )
    Invoke-Command -ComputerName $ComputerName -ScriptBlock {
        $CacheFiles = Get-ChildItem -Path $env:windir\Temp -File -Filter *.cache
        $CacheFiles | Remove-Item
    }
}

function Get-SQLBackupJobStatus {
    param (
        $ComputerName
    )
    Invoke-SQL -dataSource $ComputerName -database Master -sqlCommand @"
SELECT session_id as SPID, command, a.text AS Query, start_time, percent_complete, dateadd(second,estimated_completion_time/1000, getdate()) as estimated_completion_time
FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) a
WHERE r.command in ('BACKUP DATABASE','RESTORE DATABASE','BACKUP LOG')
"@
}

function Get-LatestOlaHalengrenMaintenanceActivity {
    param (
        $ComputerName
    )
    Invoke-SQL -dataSource $ComputerName -database Master -sqlCommand @"
SELECT TOP 10 [ID]
      ,[StartTime]
      ,[EndTime]
      ,[DatabaseName]
      ,[SchemaName]
      ,[ObjectName]
      ,[ObjectType]
      ,[IndexName]
      ,[IndexType]
      ,[ExtendedInfo]
      ,[Command]
      ,[CommandType]
      ,[ErrorNumber]
      ,[ErrorMessage]
  FROM [master].[dbo].[CommandLog]
order by ID desc
"@

}

function Get-GhostRecordCounts {
<#
.EXAMPLE

Get-GhostRecordCounts -ComputerName SQLServerName -DatabaseName DatabaseName -TableName dbo.Table
#>
    param (
        $ComputerName,
        $DatabaseName,
        $TableName,
        $SchemaName = "dbo"
    )
    Invoke-SQL -dataSource $ComputerName -database $DatabaseName -ConvertFromDataRow -sqlCommand @"
SELECT * FROM sys.dm_db_index_physical_stats
(DB_ID(N'$DatabaseName'), OBJECT_ID(N'$DatabaseName.$SchemaName.$TableName'), NULL, NULL , 'DETAILED')
"@ | where ghost_record_count
}

function Get-SQLTableNameFromObjectID {
    param (
        $ComputerName,
        [int]$DatabaseID,
        [int]$TableObjectID
    )
    Invoke-SQL -dataSource $ComputerName -database Master -ConvertFromDataRow -sqlCommand @"
select OBJECT_NAME ($TableObjectID, $DatabaseID)
"@ | select -ExpandProperty Column1
}

function Get-SQLIndexNameFromID {
    param (
        $ComputerName,
        $IndexID,
        $TableObjectID
    )
    Invoke-SQL -dataSource $ComputerName -database $DatabaseName -sqlCommand @"
SELECT name FROM sys.indexes WHERE object_id = $ObjectID and index_id = $IndexID
"@
}

function Get-SQLSPWho2 {
    param (
        $ComputerName
    )
   
    Invoke-SQL -dataSource $ComputerName -database Master -sqlCommand "exec sp_who2"
}

function Get-GhostCleanUpTasksRunning {
    param (
        $ComputerName
    )
    Get-SQLSPWho2 -ComputerName $ComputerName | 
    where command -Match ghost
}

function Get-ConnectionsToSpecificDatabase {
    param (
        $ComputerName,
        $DatabaseName
    )
    Get-SQLSPWho2 -ComputerName $ComputerName | 
    where DBName -EQ $DatabaseName
}

function Get-DatabaseTableNames {
    param (
        $ComputerName,
        $DatabaseName
    )
   
    Invoke-SQL -dataSource $ComputerName -database $DatabaseName -ConvertFromDataRow -sqlCommand @"
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='$DatabaseName'
"@ | select -ExpandProperty Table_Name
}

function Invoke-FullScanOfAllTablesInDatabase {
    [cmdletbinding()]
    param (
        $ComputerName,
        $DatabaseName
    )
    foreach ($TableName in $(Get-DatabaseTableNames @PSBoundParameters | Sort)) {
        Write-Verbose "Starting $TableName"
        Invoke-SQL -dataSource $ComputerName -database $DatabaseName -sqlCommand @"
SELECT * FROM $TableName with (nolock)
"@ | Out-Null
        Write-Verbose "Finished $TableName"
    }

}