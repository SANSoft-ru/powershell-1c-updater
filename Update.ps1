Param (
	[Parameter (Mandatory=$true, Position=1)]
	[string]$Mode,

	[Parameter (Mandatory=$true, Position=2)]
	[string]$VersionList,

	[Parameter (Mandatory=$true, Position=3)]
	[string]$ConfigFileName,

	[Parameter (Mandatory=$false, Position=4)]
	[bool]$TestRun
)

# ���������� ���������� � ���������
# .\Update.ps1 -Mode A -VersionList "3_0_157_32" -ConfigFileName updList-test.txt -TestRun $true
$Debug = $TestRun
$ParamIsGood = $true
$DbList = New-Object 'System.Collections.Generic.Dictionary[string,string]'
$Versions = New-Object 'System.Collections.Generic.List[string[]]'
$UpdateResults = New-Object 'System.Collections.Generic.List[string]'
$WorkPath = ""
$Exe1cv8 = ""
$Templates = ""
$LogFileName = ""
$global:ConnectionTo1c = $null
$global:LastConnectionString = ""
$COMConnectorId = "v83.COMConnector"
$UnlockCode = "Powershell_��������������������������������"
$Delay = $(if ($Debug) { 5 } else { 600 })
$WaitUsers = $(if ($Debug) { 90 } else { 180 })

$Mode = $Mode.ToUpper()

Function WriteLog([string]$text, [string]$level)
{
	$timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
	if ($text) {
		$text = " " + $text.Trim()
	}
	if ($level) {
		$level = " [" + $level.Trim() + "]"
	}
	$messageText = $timestamp + $level + $text

	Write-Output $messageText | Out-File $LogFileName -Append
}

Function WriteStartMessage([string]$text)
{
	WriteLog -text "$text ������..."
	Return Get-Date
}

Function WriteStopMessage([string]$text, [int]$result, [DateTime]$startTime)
{
    $difference = ((Get-Date) - $startTime).ToString()
	WriteLog -text "$text �������. ��� �������� $result. ����� ���������� $difference"
}

Function Run1CWithWait([string]$params1c)
{
	$additionalParameters = " /UC""$UnlockCode"" /DisableStartupMessages /DisableStartupDialogs /Out ""$LogFileName"" -NoTruncate"

	$processInfo = New-Object System.Diagnostics.ProcessStartInfo
	$processInfo.FileName = $Exe1cv8
	$processInfo.RedirectStandardError = $false
	$processInfo.RedirectStandardOutput = $false
	$processInfo.UseShellExecute = $false
	$processInfo.Arguments = $params1c + $additionalParameters

	$cmdLine = """{0}"" {1}" -f $processInfo.FileName, $processInfo.Arguments
	WriteLog $cmdLine

	$process = New-Object System.Diagnostics.Process
	$process.StartInfo = $processInfo
	$process.Start() | Out-Null

	if ($Debug) {
		$exitCode = 0
	} else {
		$process.WaitForExit()
		$exitCode = $process.ExitCode
	}

	Return $exitCode
}

Function ConnectTo1C([string]$connectionString)
{
	if ($global:ConnectionTo1c -and $global:LastConnectionString -eq $connectionString) {
		WriteLog "���������� ������ �����������: $connectionString"
		return $global:ConnectionTo1c
	}

	$global:ConnectionTo1c = $null
	$global:LastConnectionString = ""

	try {
		WriteLog "������������ � 1�: $connectionString"
		$Connector = New-Object -ComObject $COMConnectorId
		$global:ConnectionTo1c = $Connector.Connect($connectionString)
		$global:LastConnectionString = $connectionString
	} catch {
		WriteLog "������ ��� �������� � ����������� COM-������� $COMConnectorId" "ERROR"
		WriteLog $_ "ERROR"
		$global:ConnectionTo1c = $null
		$global:LastConnectionString = ""
	} finally {
		$Connector = $null
	}

	return $global:ConnectionTo1c
}

Function GetProperty([System.__ComObject]$obj,[string]$propertyName)
{
	$property = $null
	try {
		$property = [System.__ComObject].InvokeMember($propertyName,[System.Reflection.BindingFlags]::GetProperty,$null,$obj,$null)
	} catch {
		$property = $null
		WriteLog "������ ��������� �������� $propertyName" "ERROR"
		WriteLog $_ "ERROR"
	}

	return $property
}

Function CallMethod([System.__ComObject]$obj,[string]$methodName,[object]$paramArray)
{
	if (!$paramArray) {
		$paramArray = $null
	}

	$retvalue = $null
	try {
		$retvalue = [System.__ComObject].InvokeMember($methodName,[System.Reflection.BindingFlags]::InvokeMethod,$null,$obj,$paramArray)
	} catch {
		$retvalue = $null
		WriteLog "������ ������ ������ $methodName" "ERROR"
		WriteLog $_ "ERROR"
	}

	return $retvalue
}

Function SetScheduledJobsDenied([string]$server1c,[string]$dbName,[string]$dbUser,[string]$dbPassword,[bool]$flag)
{
	$oldStatus = $false

	try {
		$Connector = New-Object -ComObject $COMConnectorId
		$AgentConnection = $Connector.ConnectAgent($server1c) 
		$Cluster = $AgentConnection.GetClusters()[0] 
		$AgentConnection.Authenticate($Cluster,"","") 
		$WorkingProcess = $AgentConnection.GetWorkingProcesses($Cluster)[0]	

		$ConnectionString = "{0}:{1}" -f $WorkingProcess.HostName, $WorkingProcess.MainPort
		WriteLog "������������ � ������� ��������: $ConnectionString"

		$WorkingProcessConnection = $Connector.ConnectWorkingProcess($ConnectionString)
		$WorkingProcessConnection.AddAuthentication($dbUser,$dbPassword)

		$ib = $WorkingProcessConnection.GetInfoBases() | Where {$_.Name -eq $dbName}

		if ($ib) {
			$oldStatus = $ib.ScheduledJobsDenied

			if ($oldStatus -ne $flag) {
				$ib.ScheduledJobsDenied = $flag
				$WorkingProcessConnection.UpdateInfoBase($ib)
				WriteLog "���� ������� ������������ ���������� � �������� $flag"
			}
		} else {
			WriteLog "�� ������� [$server1c] �� ������� �������������� ���� [$dbName]" "ERROR"
		}
	} catch {
		WriteLog "������ ��������� ����� [$flag] ������� ������������ �������" "ERROR"
		WriteLog $_ "ERROR"
	} finally {
		$ib = $null
		$WorkingProcessConnection = $null
		$WorkingProcess = $null
		$Cluster = $null
		$AgentConnection = $null
		$Connector = $null
	}

	return $oldStatus
}

Function Run1C([string]$params1c)
{
	$processInfo = New-Object System.Diagnostics.ProcessStartInfo
	$processInfo.FileName = $Exe1cv8
	$processInfo.RedirectStandardError = $false
	$processInfo.RedirectStandardOutput = $false
	$processInfo.UseShellExecute = $false
	$processInfo.Arguments = $params1c

	$cmdLine = """{0}"" {1}" -f $processInfo.FileName, $processInfo.Arguments
	WriteLog $cmdLine

	$process = New-Object System.Diagnostics.Process
	$process.StartInfo = $processInfo
	$process.Start() | Out-Null
}

Function ForceReleaseComConnection()
{
	if ($global:ConnectionTo1c) {
		[System.Runtime.InteropServices.Marshal]::ReleaseComObject([System.__ComObject]$global:ConnectionTo1c) | out-null
		[System.GC]::Collect()
		[System.GC]::WaitForPendingFinalizers()
	}
	
	$global:ConnectionTo1c = $null
	$global:LastConnectionString = ""
}

Function ToLeftStringWithWidth([object]$obj,[int]$width)
{
	$objString = " "
	if ($obj) {
		$objString = $obj.ToString()
	} 
	Return $objString.PadRight($width).Substring(0,$width)
}

$global:ConnectionTo1c = $null

Function DoUpdate([string]$CounterText,[string]$DbName,[string]$DbConnection,[System.Collections.Generic.List[string[]]]$Versions)
{
	$DbPath = ""
	$DbUser = ""
	$DbPassword = ""
	$DbConnectionString = ""
	$DbConnectionString1c = ""
	$returnCode = 0
	$UpdateSuccess = $false

	$DbType = "/S"
	$dbServer = ""
	$dbName = ""
	$ScheduledJobsDeniedStatus = $false
	
	$DbConnectionParts = $DbConnection.Split("^")
	
	$DbPath = $DbConnectionParts[0].Trim()

	WriteLog "-------------------------------------------------------"
	WriteLog "��������� [$CounterText] $DbPath"

	# ���� � ��
	if ($DbPath.StartsWith("\\") -or ($DbPath.Substring(1, 2) -eq ":\")) {
		$DbType = "/F"
		$DbConnectionString1c = "File=""$DbPath"""
		if (!(Test-Path "$DbPath\1Cv8.1CD")) {
			WriteLog "�� ������ ���� �� 1� � ����� $DbPath" "ERROR"
			WriteLog
			ForceReleaseComConnection
			Return $UpdateSuccess
		}
	} else {
		$DbPathParts = $DbPath.Split("\")
		if ($DbPathParts.Count -eq 2) {
			$dbServer = $DbPathParts[0].Trim()
			$dbName = $DbPathParts[1].Trim()
			$DbConnectionString1c = "Srvr=""$dbServer"";Ref=""$dbName"""
		} else {
			WriteLog "������� ������� ��������� ����������� � �� $DbPath" "ERROR"
			WriteLog
			ForceReleaseComConnection
			Return $UpdateSuccess
		}
	}

	# ����� ������������ 1�
	if ($DbConnectionParts.Count -ge 2) {
		if ($DbConnectionParts[1].Trim()) {
			$DbUser = $DbConnectionParts[1].Trim()
		}
	}

	# ������ ������������ 1�
	if ($DbConnectionParts.Count -ge 3) {
		if ($DbConnectionParts[2].Trim()) {
			$DbPassword = $DbConnectionParts[2].Trim()
		}
	}

	$DbConnectionString = "{0} {1}{2}{3}" -f $DbType, $DbPath, $(if ($DbUser) { " /WA- /N"""+$DbUser+"""" } else { "" }), $(if ($DbPassword) { " /P"""+$DbPassword+"""" } else { "" })
	$DbConnectionString1c = "{0};Usr=""{1}"";Pwd=""{2}"";UC={3}" -f $DbConnectionString1c, $DbUser, $DbPassword, $UnlockCode
	$BackupFileName = """{0}\{1}_{2}.dt.dll""" -f $WorkPath, $DbName, (Get-Date).ToString("yyyyMMdd-HHmmss")

	# ���������� ������ �������������
	WriteLog "���������� ������ �������������..."
	$SessionCount = 5
	$Message = ""
	try {
		$connection = ConnectTo1C $DbConnectionString1c
		$IbConnections = GetProperty $connection "������������"
		$RetValue = CallMethod $IbConnections "������������������������������" @("� ����� � �������������� ���������� ������������", $UnlockCode)

		$BlockParams = [System.__ComObject].InvokeMember("��������������������������",[System.Reflection.BindingFlags]::InvokeMethod,$null,$IbConnections,@($true))

		$DisconnectionInterval = GetProperty $BlockParams "���������������������������������������������"
		if ($DisconnectionInterval -gt 1800) {
			$DisconnectionInterval = 1800
		}
		WriteLog "���������������������������������������������: $DisconnectionInterval"

		$DisconnectionStartDateTime = GetProperty $BlockParams "������"
		WriteLog "������: $DisconnectionStartDateTime"

		$DisconnectionEnabled = GetProperty $BlockParams "�����������"
		WriteLog "�����������: $DisconnectionEnabled"
		if ($DisconnectionEnabled) {
			$SessionCount = GetProperty $BlockParams "�����������������"
			WriteLog "�����������������: $SessionCount"

			if ($SessionCount -gt 1) {
				# �������� ������ �������������
				$DisconnectionEndDateTime = $DisconnectionStartDateTime.AddSeconds($DisconnectionInterval)
				WriteLog "������� ��: $DisconnectionEndDateTime"
				while ((Get-Date) -lt $DisconnectionEndDateTime -and $SessionCount -gt 1) {
					Start-Sleep -s $WaitUsers
					$SessionCount = CallMethod $IbConnections "�����������������������������������" @($false)
					WriteLog "�����������������: $SessionCount"
				}
			}
		}

		if ($SessionCount -gt 1) {
			$RetValue = CallMethod $IbConnections "����������������������������"
			$Message = CallMethod $IbConnections "������������������������������"

			WriteLog "�� ������� ��������� ������ �������������" "ERROR"
			WriteLog $Message "ERROR"
		} 
	} catch {
		WriteLog "������ ���������� ������ �������������" "ERROR"
		WriteLog $_ "ERROR"
		$SessionCount = 5
	} finally {
		$BlockParams = $null
		$IbConnections = $null
		$connection = $null
	}
	WriteLog
	ForceReleaseComConnection

	if ($SessionCount -gt 1) {
		Return $UpdateSuccess
	}
	
	$startTime = WriteStartMessage "DumpIB"
	$returnCode = Run1CWithWait "CONFIG $DbConnectionString /DumpIB $BackupFileName"
	WriteStopMessage "DumpIB" $returnCode $startTime
	
	if ($returnCode -ne 0) {
		WriteLog "������ ���������� �����������" "ERROR"
		WriteLog
		ForceReleaseComConnection
		Return $UpdateSuccess
	} else {
		WriteLog
	}

	# ��������� ���������� ������������ �������, ���� � ���
	if ($DbType -eq "/S") {
		WriteLog "������� ��������� ����� ������� ������������ �������..."
		$ScheduledJobsDeniedStatus = SetScheduledJobsDenied $dbServer $dbName $DbUser $DbPassword $true
		WriteLog "������ �������� ����� ������� ������������ �������: $ScheduledJobsDeniedStatus"
		WriteLog
	}

	# �������� ������
	WriteLog "������� �������� ������..."
	try {
		$connection = ConnectTo1C $DbConnectionString1c
		$UpdConfSeverCall = GetProperty $connection "����������������������������������"
		$RetValue = CallMethod $UpdConfSeverCall "���������������������������" 
		WriteLog "������� 1�: ����������������������������������.���������������������������(). ���������: $RetValue"
	} catch {
		WriteLog "������ �������� ������" "ERROR"
		WriteLog $_ "ERROR"
	} finally {
		$UpdConfSeverCall = $null
		$connection = $null
	}
	WriteLog

	$hasError = $false
	ForceReleaseComConnection
	
	$ind=0
	ForEach ($Version In $Versions) {
		$ind++
		$counterText = "{0}/{1} {2}" -f $ind, $Versions.Count, $Version[0]
		$startTime = WriteStartMessage "[$counterText] UpdateCfg"
		$versionPath = $Version[1]

		$returnCode = Run1CWithWait "CONFIG $DbConnectionString /UpdateCfg ""$versionPath"""
		WriteStopMessage "UpdateCfg" $returnCode $startTime
		WriteLog

		if ($returnCode -eq 0) {
			$startTime = WriteStartMessage "[$counterText] UpdateDBCfg"
			$returnCode = Run1CWithWait "CONFIG $DbConnectionString /UpdateDBCfg -server"
			WriteStopMessage "UpdateDBCfg" $returnCode $startTime
			WriteLog

			if ($returnCode -ne 0) {
				$hasError = $true
				Break
			} else {
				$hasError = $false
			}
		} else {
			$hasError = $true
		}
	}

	if (!$hasError) {
		# ������ ������������ ����������
		WriteLog "������ ������������ ����������..."
		try {
			$connection = ConnectTo1C $DbConnectionString1c
			$UpdIbSeverCall = GetProperty $connection "����������������������������������������"
			$RetValue = CallMethod $UpdIbSeverCall "�������������������������������������" @($false) 
			WriteLog "������� 1�: ����������������������������������������.�������������������������������������(). ���������: $RetValue"

			$UpdConf = GetProperty $connection "����������������������"
			$RetValue = CallMethod $UpdConf "�������������������" @($true,"",$DbUser) 
			WriteLog "������� 1�: ����������������������.�������������������(). ���������: $RetValue"
			$UpdateSuccess = $true
		} catch {
			WriteLog "������ ������� ������������ ����������" "ERROR"
			WriteLog $_ "ERROR"
		} finally {
			$UpdConf = $null
			$UpdIbSeverCall = $null
			$connection = $null
		}
		WriteLog
	} else {
		WriteLog "�� ����� ���������� �������� ������" "ERROR"
		WriteLog
	}

	# ���������� ������ �������������
	WriteLog "���������� ������ �������������..."
	try {
		$connection = ConnectTo1C $DbConnectionString1c
		$IbConnections = GetProperty $connection "������������"
		$RetValue = CallMethod $IbConnections "����������������������������"
	} catch {
		WriteLog "������ ���������� ������ �������������" "ERROR"
		WriteLog $_ "ERROR"
	} finally {
		$IbConnections = $null
		$connection = $null
	}
	WriteLog

	if ($UpdateSuccess) {
		WriteLog "������ ENTERPRISE..."
		Run1C "ENTERPRISE $DbConnectionString"
		WriteLog
	}

	if (!$ScheduledJobsDeniedStatus) {
		WriteLog "����� $Delay ���."
		Start-Sleep -s $Delay
		WriteLog "������� �������������� ����� ������� ������������ �������..."
		$rc = SetScheduledJobsDenied $dbServer $dbName $DbUser $DbPassword $false
		WriteLog
	} else {
		WriteLog "�������������� ����� ������� ������������ ������� �� ���������"
		WriteLog
	}

	WriteLog

	ForceReleaseComConnection
	
	Return $UpdateSuccess
}

# ������ ���������� �������

if ($Mode -eq "A") {
	$ModeName = "Accounting"
	$ModeDescription = "�����������"
} elseif ($Mode -eq "H") {
	$ModeName = "HRM"
	$ModeDescription = "���"
} else {
	Write-Warning("�������� Mode ������ ���� ����� A (�����������) ��� H (���)")
	$ParamIsGood = $false
}

if (!(Test-Path $ConfigFileName)) {
	Write-Warning("���� ������������ [$ConfigFileName] �� ������")
	$ParamIsGood = $false
} else {
	$ConfigLines = Get-Content -Path $ConfigFileName | Where {!$_.StartsWith("#") -and $_}	
	ForEach ($ConfigLine In $ConfigLines) {
		$ConfigLineParts = $ConfigLine.Split("=")
		if ($ConfigLineParts.Count -eq 2) {
			$Key = $ConfigLineParts[0]
			$Value = $ConfigLineParts[1]
			$KeyName = $Key.ToLower()
			if ($KeyName -eq "mode") {
				if (!($Value.ToLower() -eq "update")) {
					Write-Warning("����� ������������ [$ConfigLine] �� ����� Update")
					$ParamIsGood = $false
				}
			} elseif ($KeyName -eq "workpath") {
				$WorkPath = $Value.Trim()
				if (!(Test-Path $WorkPath)) {
					Write-Warning("������� ����� �� ������������ [$WorkPath] �� �������")
					$ParamIsGood = $false
				}
			} elseif ($KeyName -eq "exepath") {
				$Exe1cv8 = $Value.Trim()+"\1cv8.exe"
				if (!(Test-Path $Exe1cv8)) {
					Write-Warning("�������� 1� �� ���� �� ������������ [$Value] �� ������")
					$ParamIsGood = $false
				}
			} elseif ($KeyName -eq "templates") {
				$Templates = $Value.Trim()+"\"+$ModeName
				if (!(Test-Path $Templates)) {
					Write-Warning("���� � ����������� [$ModeName] �� ������������ [$Value] �� ������")
					$ParamIsGood = $false
				}
			} else {
				If (!$DbList.ContainsKey($Key)) {
					if (($mode -eq "a" -and !$KeyName.StartsWith("zk-")) -or ($mode -eq "h" -and $KeyName.StartsWith("zk-"))) {
						if ($Value) {
							$DbList.Add($Key, $Value.Trim())
						} else {
							Write-Warning("� ������������ �� ������� �� [$Key]")
							$ParamIsGood = $false
						}
					}
				} else {
					Write-Warning("� ������������ ����������� �� [$Key]")
					$ParamIsGood = $false
 				}
			}
		} else {
			Write-Warning("������ ������������ [$ConfigLine] �� ������������ ������� Key=Value")
			$ParamIsGood = $false
		}
	}
}

if (!$WorkPath) {
	Write-Warning("� ������������ �� ������� ������� �����. �������� WorkPath")
	$ParamIsGood = $false
}

if (!$Exe1cv8) {
	Write-Warning("� ������������ �� ������� ���� � ���������� 1�. �������� EXEPath")
	$ParamIsGood = $false
}

if (!$Templates) {
	Write-Warning("� ������������ �� ������� ���� � ������ ����������. �������� Templates")
	$ParamIsGood = $false
} else {
	ForEach ($Version In $VersionList.Split(",")) {
		$versionTrim = $Version.Trim()
		$VersionPath = $Templates + "\" + $versionTrim + "\1cv8.cfu"
		if (!(Test-Path $VersionPath)) {
			Write-Warning("���� ���������� ������ [$VersionPath] �� ������")
			$ParamIsGood = $false
		} else {
			$Versions.Add(@($versionTrim,$VersionPath))
		}
	}
}

if ($DbList.Count -eq 0) {
	Write-Warning("� ������������ �� ������� ������ ����������� � �� 1�")
	$ParamIsGood = $false
}

if ($Versions.Count -eq 0) {
	Write-Warning("�� ������� ������ ���������� 1�")
	$ParamIsGood = $false
}

if (!$ParamIsGood) {
	Break
}

# �������������
$LogFileName = $WorkPath + "\" + (Get-Date).ToString("yyyyMMdd-HHmmss") + ".log"
$Exe1cv8 = $(if ($Debug) { "calc.exe" } else { $Exe1cv8 })

WriteLog "SANSoft(c) 2024. ���������� ��� 1�"
WriteLog "=================================="
WriteLog
WriteLog "����� �������: $Debug"
WriteLog "����� ����������: [$Mode] $ModeDescription"
WriteLog "���������� ��� � ����������: $($DbList.Count)"
ForEach ($DbKey In $DbList.Keys) {
	WriteLog "[$DbKey]"
}
WriteLog

WriteLog "���������� ����������: $($Versions.Count)"
ForEach ($Version In $Versions) {
	$versionPath = $Version[1]
	WriteLog "[$versionPath]"
}
WriteLog

$tableWidth = 46
$tableRowTemplate = "{0} | {1} | {2}"
$index=0
$overallStartTime = Get-Date
ForEach ($DbKey In $DbList.Keys) {
	$index++
	$startTime = Get-Date
	$counter = "{0}/{1}" -f $index, $DbList.Count
	$updResult = DoUpdate -CounterText $counter -DbName $DbKey -DbConnection $DbList[$DbKey] -Versions $Versions
	$span = ((Get-Date) - $startTime).ToString()
	$updResultString = $tableRowTemplate -f (ToLeftStringWithWidth $DbKey 15),(ToLeftStringWithWidth $updResult 9), $span 
	$UpdateResults.Add($updResultString)
}

WriteLog
WriteLog "����� ����������:"
WriteLog "=".PadRight($tableWidth,"=")
WriteLog ($tableRowTemplate -f (ToLeftStringWithWidth "����" 15),(ToLeftStringWithWidth "���������" 9), "�����") 
WriteLog "=".PadRight($tableWidth,"=")
ForEach ($updResult In $UpdateResults) {
	WriteLog $updResult
}
WriteLog "=".PadRight($tableWidth,"=")

$span = ((Get-Date) - $overallStartTime).ToString()
WriteLog "������ ����� ���������� $span"

WriteLog
WriteLog
WriteLog "SANSoft(c) 2024. ���������� ��� 1�"

ForceReleaseComConnection
