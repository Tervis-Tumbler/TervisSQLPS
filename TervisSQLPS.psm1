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

function Enable-SQLRemoteAccess {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        $InstanceName = "MSSQLSERVER",
        [ValidateSet("x86","x64")]$Architecture = "x64"
    )
    begin {
        $MicrosoftSQLServerRegistryPath = Get-MicrosoftSQLServerRegistryPath -Architecture $Architecture
        $SQLTCPKeyRelativePath = "\MSSQLServer\SuperSocketNetLib\Tcp"
    }
    process {
        Write-Verbose "Enabling SQL remote access"
        Invoke-Command -ComputerName $ComputerName -ScriptBlock {        
            $SQLVersionAndInstanceName = Get-ChildItem -Path $Using:MicrosoftSQLServerRegistryPath | 
            where PSChildName -Match "\.$Using:InstanceName" |
            select -ExpandProperty PSChildName

            $SQLTCPKeyPath = $Using:MicrosoftSQLServerRegistryPath + "\$SQLVersionAndInstanceName" + $Using:SQLTCPKeyRelativePath
            $Enabled = Get-ItemProperty -Path $SQLTCPKeyPath -Name Enabled |
            select -ExpandProperty Enabled

            if (-not $Enabled) {
                Set-ItemProperty -Path $SQLTCPKeyPath -Name Enabled -Value 1
                $ServiceName = if ($Using:InstanceName) {
                    "MSSQL`$$Using:InstanceName"
                } else {
                    "MSSQLServer"
                }               
                Restart-Service -Name $ServiceName -Force
            }
        }
    }
}

function Set-SQLTCPEnabled {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        $InstanceName = "MSSQLServer",
        [ValidateSet("x86","x64")]$Architecture = "x64"
    )
    process {
        Set-SQLSuperSocketNetLibRegistryProperty -ComputerName $ComputerName -InstanceName $InstanceName -Architecture $Architecture -Name Enabled -RelativePath "\Tcp" -Value 1
    }
}

function Get-SQLTCPEnabled {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        $InstanceName = "MSSQLServer",
        [ValidateSet("x86","x64")]$Architecture = "x64"
    )
    process {
        Get-SQLSuperSocketNetLibRegistryProperty -ComputerName $ComputerName -InstanceName $InstanceName -Architecture $Architecture -Name Enabled -RelativePath "\Tcp"
    }
}

function Get-SQLNetTcpConnection {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName
    )
    process {
        $CimSession = New-CimSession -ComputerName $ComputerName
        Get-NetTCPConnection -CimSession $CimSession -LocalPort 1433
        Remove-CimSession -CimSession $CimSession
    }
}

function Get-SQLNetConnection {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName
    )
    process {
        $CimSession = New-CimSession -ComputerName $ComputerName
        Get-NetTCPConnection -CimSession $CimSession -LocalPort 1433
        Remove-CimSession -CimSession $CimSession
    }
}

function New-SQLNetFirewallRule {
    param (
        [Parameter(ValueFromPipelineByPropertyName)]$ComputerName
    )
    process {
        Invoke-Command -ComputerName $ComputerName -ScriptBlock {
            $FirewallRule = Get-NetFirewallRule -Name "MSSQL" -ErrorAction SilentlyContinue
            if (-not $FirewallRule) {
                New-NetFirewallRule -Name "MSSQL" -DisplayName "MSSQL" -Direction Inbound -LocalPort 1433 -Protocol TCP -Action Allow -Group MSSQL | Out-Null
            }
        }
    }
}

function Get-SQLTCPIPAllTcpPort {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        $InstanceName = "MSSQLServer",
        [ValidateSet("x86","x64")]$Architecture = "x64"
    )
    process {
        Get-SQLSuperSocketNetLibRegistryProperty -ComputerName $ComputerName -InstanceName $InstanceName -Architecture $Architecture -Name TcpPort -RelativePath "\Tcp\ipall"
    }
}

function Set-SQLTCPIPAllTcpPort {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        $InstanceName = "MSSQLServer",
        [ValidateSet("x86","x64")]$Architecture = "x64",
        $TcpPort = 1433
    )
    process {
        Set-SQLSuperSocketNetLibRegistryProperty -ComputerName $ComputerName -InstanceName $InstanceName -Architecture $Architecture -Name TcpPort -RelativePath "\Tcp\ipall" -Value $TcpPort
    }
}

function Get-SQLSuperSocketNetLibRegistryPropertyPath {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        $InstanceName = "MSSQLSERVER",
        [ValidateSet("x86","x64")]$Architecture = "x64",
        [Parameter(Mandatory)]$RelativePath
    )
    begin {
        $MicrosoftSQLServerRegistryPath = Get-MicrosoftSQLServerRegistryPath -Architecture $Architecture
        $SuperSocketNetLibRelativePath = "\MSSQLServer\SuperSocketNetLib"
    }
    process {
        Invoke-Command -ComputerName $ComputerName -ScriptBlock {        
            $SQLVersionAndInstanceName = Get-ChildItem -Path $Using:MicrosoftSQLServerRegistryPath | 
            where PSChildName -Match "\.$Using:InstanceName" |
            select -ExpandProperty PSChildName

            $SQLKeyPath = $Using:MicrosoftSQLServerRegistryPath + "\$SQLVersionAndInstanceName" + $Using:SuperSocketNetLibRelativePath + $Using:RelativePath
            $SQLKeyPath
        }
    }

}

function Get-SQLSuperSocketNetLibRegistryProperty {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        $InstanceName = "MSSQLSERVER",
        [ValidateSet("x86","x64")]$Architecture = "x64",
        [Parameter(Mandatory)]$Name,
        [Parameter(Mandatory)]$RelativePath
    )
    process {
        $SQLKeyPath = Get-SQLSuperSocketNetLibRegistryPropertyPath -ComputerName $ComputerName -InstanceName $InstanceName -Architecture $Architecture -RelativePath $RelativePath
        Invoke-Command -ComputerName $ComputerName -ScriptBlock {        
            Get-ItemProperty -Path $Using:SQLKeyPath -Name $Using:Name |
            select -ExpandProperty $Using:Name
        }
    }
}

function Set-SQLSuperSocketNetLibRegistryProperty {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        $InstanceName = "MSSQLSERVER",
        [ValidateSet("x86","x64")]$Architecture = "x64",
        [Parameter(Mandatory)]$Name,
        [Parameter(Mandatory)]$RelativePath,
        [Parameter(Mandatory)]$Value
    )
    process {
        $SQLKeyPath = Get-SQLSuperSocketNetLibRegistryPropertyPath -ComputerName $ComputerName -InstanceName $InstanceName -Architecture $Architecture -RelativePath $RelativePath
        Invoke-Command -ComputerName $ComputerName -ScriptBlock {        
            $CurrentValue = Get-ItemProperty -Path $Using:SQLKeyPath -Name $Using:Name |
            select -ExpandProperty $Using:Name

            if ($CurrentValue -ne $Using:Value) {
                Set-ItemProperty -Path $Using:SQLKeyPath -Name $Using:Name -Value $Using:Value
                $ServiceName = if ($Using:InstanceName) {
                    "MSSQL`$$Using:InstanceName"
                } else {
                    "MSSQLServer"
                }               
                Restart-Service -Name $ServiceName -Force
            }
        }
    }
}

function Get-MicrosoftSQLServerRegistryPath {
    param (
        [ValidateSet("x86","x64")]$Architecture = "x64"        
    )
    if ($Architecture -eq "x64") {
            "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server"
    } elseif ($Architecture -eq "x86") {
            "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Microsoft SQL Server"
    }
}

function Set-SQLSecurityBuiltInAdministratorsWithSysman {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName
    )
    process {
        $Command = @"
USE [master]
GO
CREATE LOGIN [BUILTIN\administrators] FROM WINDOWS WITH DEFAULT_DATABASE=[master]
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [BUILTIN\administrators]
GO
"@
        Invoke-SQL -dataSource $ComputerName -database Master -sqlCommand $Command
    }
}