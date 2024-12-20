﻿Param (
	[Parameter (Mandatory=$true, Position=1)]
	[string]$Mode,

	[Parameter (Mandatory=$true, Position=2)]
	[string]$VersionList,

	[Parameter (Mandatory=$true, Position=3)]
	[string]$ConfigFileName,

	[Parameter (Mandatory=$false, Position=4)]
	[bool]$TestRun
)

# Образец запуска
# Если установлена 32бит 1С-Платформа, то запускать скрипт нужно в 32бит Powershell
# Если параметр TestRun равен $true, то реального запуска процесса 1с не происходит
# вместо 1С будет запущен калькулятор. В диспетчере задач будет видно параметры его запуска
# .\Update.ps1 -Mode A -VersionList "3_0_152_15,3_0_157_32" -ConfigFileName updList.txt -TestRun $true

# Глобальные переменные и константы
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
$UnlockCode = "Powershell_ПакетноеОбновлениеКонфигурацииИБ"
$Delay = $(if ($Debug) { 5 } else { 60 })
$MaxDelayCount = 60
$WaitUsers = $(if ($Debug) { 90 } else { 180 })

$Mode = $Mode.ToUpper()

# Вывод строки текста в лог-файл
# text - Текст для вывода, не обязательный параметр, по умолчанию - пустой
# level - Уровень серьёзности, не обязательный параметр, по умолчанию - пустой
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

# Вывод строки текста в консоль
# text - Текст для вывода, не обязательный параметр, по умолчанию - пустой
# color - Цвет текста, не обязательный параметр, по умолчанию - Жёлтый
# alsoInLog - Флаг попутного вывода информации в лог-файл, не обязательный параметр, по умолчанию - да
Function WriteConsole([string]$text, [ConsoleColor]$color = [ConsoleColor]::Yellow, [bool]$alsoInLog = $true)
{
	if ($text) {
		$text = $text.Trim()
	}

	Write-Host -Object $text -ForegroundColor $color

	if ($alsoInLog) {
		$level = $(if ($color -eq [ConsoleColor]::Red) { "ERROR" } else { "" })

		WriteLog -text $text -level $level
	}
}

# Вывод стартовой строки текста какого-то процесса в лог-файл
# text - Текст для вывода, обязательный параметр, по умолчанию - пустой
# Возвращает время старта
Function WriteStartMessage([string]$text)
{
	WriteConsole -text "$text начало..."
	Return Get-Date
}

# Вывод строки текста об окончании какого-то процесса в лог-файл
# дополнительно выводятся в лог данные о результатах выполнения
# и продолжительность выполнения процесса
# text - Текст для вывода, обязательный параметр, по умолчанию - пустой
# result - Результат выполнения процесса, обязательный параметр, по умолчанию - 0
# startTime - Время старта процесса для расчёта продолжительности его выполнения, обязательный параметр
Function WriteStopMessage([string]$text, [int]$result, [DateTime]$startTime)
{
    $difference = ((Get-Date) - $startTime).ToString()
	WriteConsole -text "$text окончен. Код возврата $result. Время выполнения $difference"
}

# Запуск процесса 1С с указанными параметрами и ожиданием результата выполнения
# params1c - Параметры запуска процесса 1С, обязательный параметр
# Возвращает результат выполнения процесса 1С
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
		WriteLog "Используем старое подключение: $connectionString"
		return $global:ConnectionTo1c
	}

	$global:ConnectionTo1c = $null
	$global:LastConnectionString = ""

	try {
		WriteLog "Подключаемся к 1С: $connectionString"
		$Connector = New-Object -ComObject $COMConnectorId
		$global:ConnectionTo1c = $Connector.Connect($connectionString)
		$global:LastConnectionString = $connectionString
	} catch {
		WriteConsole "Ошибка при создании и подключении COM-Объекта $COMConnectorId" -color ([ConsoleColor]::Red)
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
		WriteConsole "Ошибка получения свойства $propertyName" -color ([ConsoleColor]::Red)
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
		WriteConsole "Ошибка вызова метода $methodName" -color ([ConsoleColor]::Red)
		WriteLog $_ "ERROR"
	}

	return $retvalue
}

Function SetScheduledJobsDenied([string]$server1c,[string]$dbName,[string]$dbUser,[string]$dbPassword,[bool]$flag)
{
	$oldStatus = $false

	if (!$Debug) {
		try {
			$Connector = New-Object -ComObject $COMConnectorId
			$AgentConnection = $Connector.ConnectAgent($server1c) 
			$Cluster = $AgentConnection.GetClusters()[0] 
			$AgentConnection.Authenticate($Cluster,"","") 
			$WorkingProcess = $AgentConnection.GetWorkingProcesses($Cluster)[0]	

			$ConnectionString = "{0}:{1}" -f $WorkingProcess.HostName, $WorkingProcess.MainPort
			WriteLog "Подключаемся к рабочем процессу: $ConnectionString"

			$WorkingProcessConnection = $Connector.ConnectWorkingProcess($ConnectionString)
			$WorkingProcessConnection.AddAuthentication($dbUser,$dbPassword)

			$ib = $WorkingProcessConnection.GetInfoBases() | Where {$_.Name -eq $dbName}

			if ($ib) {
				$oldStatus = $ib.ScheduledJobsDenied

				if ($oldStatus -ne $flag) {
					$ib.ScheduledJobsDenied = $flag
					$WorkingProcessConnection.UpdateInfoBase($ib)
					WriteLog "Флаг запрета регламентных заданий установлен в значение $flag"
				}
			} else {
				WriteConsole "На сервере [$server1c] не найдена информационная база [$dbName]" -color ([ConsoleColor]::Red)
			}
		} catch {
			WriteConsole "Ошибка установки флага [$flag] запрета регламентных заданий" -color ([ConsoleColor]::Red)
			WriteLog $_ "ERROR"
		} finally {
			$ib = $null
			$WorkingProcessConnection = $null
			$WorkingProcess = $null
			$Cluster = $null
			$AgentConnection = $null
			$Connector = $null
		}
	}

	return $oldStatus
}

# Запуск процесса 1С с указанными параметрами без ожидания результата выполнения
# params1c - Параметры запуска процесса 1С, обязательный параметр
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
	if ($obj -or ($obj -eq $false)) {
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
	WriteConsole "Обновляем [$CounterText] $DbPath"

	# Путь к БД
	if ($DbPath.StartsWith("\\") -or ($DbPath.Substring(1, 2) -eq ":\")) {
		$DbType = "/F"
		$DbConnectionString1c = "File=""$DbPath"""
		if (!(Test-Path "$DbPath\1Cv8.1CD")) {
			WriteConsole "Не найден файл БД 1С в папке $DbPath" -color ([ConsoleColor]::Red)
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
			WriteConsole "Неверно указаны параметры подключения к БД $DbPath" -color ([ConsoleColor]::Red)
			WriteLog
			ForceReleaseComConnection
			Return $UpdateSuccess
		}
	}

	# Логин пользователя 1С
	if ($DbConnectionParts.Count -ge 2) {
		if ($DbConnectionParts[1].Trim()) {
			$DbUser = $DbConnectionParts[1].Trim()
		}
	}

	# Пароль пользователя 1С
	if ($DbConnectionParts.Count -ge 3) {
		if ($DbConnectionParts[2].Trim()) {
			$DbPassword = $DbConnectionParts[2].Trim()
		}
	}

	$DbConnectionString = "{0} {1}{2}{3}" -f $DbType, $DbPath, $(if ($DbUser) { " /WA- /N"""+$DbUser+"""" } else { "" }), $(if ($DbPassword) { " /P"""+$DbPassword+"""" } else { "" })
	$DbConnectionString1c = "{0};Usr=""{1}"";Pwd=""{2}"";UC={3}" -f $DbConnectionString1c, $DbUser, $DbPassword, $UnlockCode
	$BackupFileName = """{0}\{1}_{2}.dt.dll""" -f $WorkPath, $DbName, (Get-Date).ToString("yyyyMMdd-HHmmss")

	# Завершение работы пользователей
	WriteConsole "Завершение работы пользователей..."
	$SessionCount = 5
	$Message = ""
	if (!$Debug) {
		try {
			$connection = ConnectTo1C $DbConnectionString1c
			$IbConnections = GetProperty $connection "СоединенияИБ"
			$RetValue = CallMethod $IbConnections "УстановитьБлокировкуСоединений" @("в связи с необходимостью обновления конфигурации", $UnlockCode)

			$BlockParams = [System.__ComObject].InvokeMember("ПараметрыБлокировкиСеансов",[System.Reflection.BindingFlags]::InvokeMethod,$null,$IbConnections,@($true))

			$DisconnectionInterval = GetProperty $BlockParams "ИнтервалОжиданияЗавершенияРаботыПользователей"
			if ($DisconnectionInterval -gt 1800) {
				$DisconnectionInterval = 1800
			}
			WriteLog "ИнтервалОжиданияЗавершенияРаботыПользователей: $DisconnectionInterval"

			$DisconnectionStartDateTime = GetProperty $BlockParams "Начало"
			WriteLog "Начало: $DisconnectionStartDateTime"

			$DisconnectionEnabled = GetProperty $BlockParams "Установлена"
			WriteLog "Блокировка сеансов: $DisconnectionEnabled"
			if ($DisconnectionEnabled) {
				$SessionCount = GetProperty $BlockParams "КоличествоСеансов"
				WriteLog "КоличествоСеансов: $SessionCount"

				if ($SessionCount -gt 1) {
					# Ожидание выхода пользователей
					$DisconnectionEndDateTime = $DisconnectionStartDateTime.AddSeconds($DisconnectionInterval)
					WriteLog "Ожидаем до: $DisconnectionEndDateTime"
					while ((Get-Date) -lt $DisconnectionEndDateTime -and $SessionCount -gt 1) {
						Start-Sleep -s $WaitUsers
						$SessionCount = CallMethod $IbConnections "КоличествоСеансовИнформационнойБазы" @($false)
						WriteLog "КоличествоСеансов: $SessionCount"
					}
				}
			}

			if ($SessionCount -gt 1) {
				$RetValue = CallMethod $IbConnections "РазрешитьРаботуПользователей"
				$Message = CallMethod $IbConnections "СообщениеОНеотключенныхСеансах"

				WriteConsole "Не удалось завершить работу пользователей" -color ([ConsoleColor]::Red)
				WriteLog $Message "ERROR"
			} 
		} catch {
			WriteConsole "Ошибка завершения работы пользователей" -color ([ConsoleColor]::Red)
			WriteLog $_ "ERROR"
			$SessionCount = 5
		} finally {
			$BlockParams = $null
			$IbConnections = $null
			$connection = $null
		}
	} else {
		$SessionCount = 1
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
		WriteConsole "Ошибка резервного копирования" -color ([ConsoleColor]::Red)
		WriteLog
		ForceReleaseComConnection
		Return $UpdateSuccess
	} else {
		WriteLog
	}

	# Установка блокировки регламентных заданий, если её нет
	if ($DbType -eq "/S") {
		WriteConsole "Попытка установки флага запрета регламентных заданий..."
		$ScheduledJobsDeniedStatus = SetScheduledJobsDenied $dbServer $dbName $DbUser $DbPassword $true
		WriteLog "Старое значение флага запрета регламентных заданий: $ScheduledJobsDeniedStatus"
		WriteLog
	}

	# Удаление патчей
	WriteConsole "Попытка удаления патчей..."
	if (!$Debug) {
		try {
			$connection = ConnectTo1C $DbConnectionString1c
			$UpdConfSeverCall = GetProperty $connection "ОбновлениеКонфигурацииВызовСервера"
			$RetValue = CallMethod $UpdConfSeverCall "УдалитьИсправленияИзСкрипта" 
			WriteLog "Команда 1с: ОбновлениеКонфигурацииВызовСервера.УдалитьИсправленияИзСкрипта(). Результат: $RetValue"
		} catch {
			WriteConsole "Ошибка удаления патчей" -color ([ConsoleColor]::Red)
			WriteLog $_ "ERROR"
		} finally {
			$UpdConfSeverCall = $null
			$connection = $null
		}
	}
	WriteLog

	$hasError = $false
	ForceReleaseComConnection
	
	$NeedSuspendedUpdate = $false
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
				$NeedSuspendedUpdate = $true
				$hasError = $false
			}
		} else {
			$hasError = $true
		}
	}

	if ($NeedSuspendedUpdate) {
		# Запуск обработчиков обновления
		WriteConsole "Запуск обработчиков обновления..."
		if (!$Debug) {
			try {
				$connection = ConnectTo1C $DbConnectionString1c
				$UpdIbSeverCall = GetProperty $connection "ОбновлениеИнформационнойБазыВызовСервера"
				$RetValue = CallMethod $UpdIbSeverCall "ВыполнитьОбновлениеИнформационнойБазы" @($false) 
				WriteLog "Команда 1с: ОбновлениеИнформационнойБазыВызовСервера.ВыполнитьОбновлениеИнформационнойБазы(). Результат: $RetValue"

				$UpdConf = GetProperty $connection "ОбновлениеКонфигурации"
				$RetValue = CallMethod $UpdConf "ЗавершитьОбновление" @($true,"",$DbUser) 
				WriteLog "Команда 1с: ОбновлениеКонфигурации.ЗавершитьОбновление(). Результат: $RetValue"
				$UpdateSuccess = $true
			} catch {
				WriteConsole "Ошибка запуска обработчиков обновления" -color ([ConsoleColor]::Red)
				WriteLog $_ "ERROR"
				$hasError = $true
			} finally {
				$UpdConf = $null
				$UpdIbSeverCall = $null
				$connection = $null
			}
		}
		WriteLog
	}

	# Разрешение работы пользователей
	WriteConsole "Разрешение работы пользователей..."
	if (!$Debug) {
		try {
			$connection = ConnectTo1C $DbConnectionString1c
			$IbConnections = GetProperty $connection "СоединенияИБ"
			$RetValue = CallMethod $IbConnections "РазрешитьРаботуПользователей"
		} catch {
			WriteConsole "Ошибка разрешения работы пользователей" -color ([ConsoleColor]::Red)
			WriteLog $_ "ERROR"
			$hasError = $true
		} finally {
			$IbConnections = $null
			$connection = $null
		}
	}
	WriteLog

	if ($UpdateSuccess) {
		WriteConsole "Запуск ENTERPRISE..."
		Run1C "ENTERPRISE $DbConnectionString"
		WriteLog
	}

	if ($NeedSuspendedUpdate -and $ScheduledJobsDeniedStatus) {
		WriteLog "Сброс флага запрета регламентных заданий..."
		$rc = SetScheduledJobsDenied $dbServer $dbName $DbUser $DbPassword $false

		try {
			$connection = ConnectTo1C $DbConnectionString1c
			$suspendedUpdateDone = $false
			$timeoutForUpdate = $MaxDelayCount * $Delay

			WriteConsole "Ожидание окончания отложенных обработчиков обновления ($timeoutForUpdate сек)..."

			For ($i = 1; $i -le $MaxDelayCount; $i++) {
				Start-Sleep -s $Delay

				$UpdateIBService = GetProperty $connection "ОбновлениеИнформационнойБазыСлужебный"
				$UpdateIBInfo = [System.__ComObject].InvokeMember("СведенияОбОбновленииИнформационнойБазы",[System.Reflection.BindingFlags]::InvokeMethod,$null,$UpdateIBService,$null)
				$EndOfUpdateTime = GetProperty $UpdateIBInfo "ВремяОкончаниеОтложенногоОбновления"
				WriteLog "Команда 1с: ВремяОкончаниеОтложенногоОбновления. Результат: $EndOfUpdateTime"

				if ($null -ne $EndOfUpdateTime) {
					$suspendedUpdateDone = $true
					Break
				}
			}
			
			If ($suspendedUpdateDone) {
				WriteConsole "Отложенное обновление успешно завершено"
			} else {
				throw "Ожидание отложенного обновления прервано по таймауту $timeoutForUpdate сек."
			}
		} catch {
			WriteLog "Ошибка ожидания оконочания обновления" "ERROR"
			WriteLog $_ "ERROR"
			$hasError = $true
		} finally {
			$UpdateIBInfo = $null
			$UpdateIBService = $null
			$connection = $null
		}
	}

	WriteConsole "Попытка восстановления флага запрета регламентных заданий..."
	$rc = SetScheduledJobsDenied $dbServer $dbName $DbUser $DbPassword $ScheduledJobsDeniedStatus
	WriteLog

	WriteLog

	ForceReleaseComConnection

	if ($hasError) {
		WriteConsole "Во время обновления возникли ошибки" -color ([ConsoleColor]::Red)
		WriteLog
	}
	
	Return !$hasError
}

# Разбор параметров запуска

if ($Mode -eq "A") {
	$ModeName = "Accounting"
	$ModeDescription = "Бухгалтерия"
} elseif ($Mode -eq "H") {
	$ModeName = "HRM"
	$ModeDescription = "ЗУП"
} elseif ($Mode -eq "U") {
	$ModeName = "TSZHBase"
	$ModeDescription = "УК"
} else {
	Write-Warning("Параметр Mode должен быть равен A (Бухгалтерия) или H (ЗУП) или U (УК)")
	$ParamIsGood = $false
}

if (!(Test-Path $ConfigFileName)) {
	Write-Warning("Файл конфигурации [$ConfigFileName] не найден")
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
					Write-Warning("Режим конфигурации [$ConfigLine] не равен Update")
					$ParamIsGood = $false
				}
			} elseif ($KeyName -eq "workpath") {
				$WorkPath = $Value.Trim()
				if (!(Test-Path $WorkPath)) {
					Write-Warning("Рабочая папка из конфигурации [$WorkPath] не найдена")
					$ParamIsGood = $false
				}
			} elseif ($KeyName -eq "exepath") {
				$Exe1cv8 = $Value.Trim()+"\1cv8.exe"
				if (!(Test-Path $Exe1cv8)) {
					Write-Warning("Бинарник 1С по пути из конфигурации [$Value] не найден")
					$ParamIsGood = $false
				}
			} elseif ($KeyName -eq "templates") {
				$Templates = $Value.Trim()+"\"+$ModeName
				if (!(Test-Path $Templates)) {
					Write-Warning("Путь к обновлениям [$ModeName] из конфигурации [$Value] не найден")
					$ParamIsGood = $false
				}
			} else {
				If (!$DbList.ContainsKey($Key)) {
					$CanAdd = $false

					# Проверим БД на префиксы
					If ($KeyName.StartsWith("zk-")) {
						$CanAdd = ($mode -eq "h")
					} 
					ElseIf ($KeyName.StartsWith("uk-")) {
						$CanAdd = ($mode -eq "u")
					}
					Else {
						$CanAdd = ($mode -eq "a")
					}

					if ($CanAdd) {
						if ($Value) {
							$DbList.Add($Key, $Value.Trim())
						} else {
							Write-Warning("В конфигурации не указана БД [$Key]")
							$ParamIsGood = $false
					    }
                    }
				} else {
					Write-Warning("В конфигурации дублируется БД [$Key]")
					$ParamIsGood = $false
				}
			}
		} else {
			Write-Warning("Строка конфигурации [$ConfigLine] не соотвествует шаблону Key=Value")
			$ParamIsGood = $false
		}
	}
}

if (!$WorkPath) {
	Write-Warning("В конфигурации не указана рабочая папка. Параметр WorkPath")
	$ParamIsGood = $false
}

if (!$Exe1cv8) {
	Write-Warning("В конфигурации не указана путь к бинарникам 1С. Параметр EXEPath")
	$ParamIsGood = $false
}

if (!$Templates) {
	Write-Warning("В конфигурации не указана путь к файлам обновлений. Параметр Templates")
	$ParamIsGood = $false
} else {
	ForEach ($Version In $VersionList.Split(",")) {
		$versionTrim = $Version.Trim()
		$VersionPath = $Templates + "\" + $versionTrim + "\1cv8.cfu"
		if (!(Test-Path $VersionPath)) {
			Write-Warning("Файл обновления версии [$VersionPath] не найден")
			$ParamIsGood = $false
		} else {
			$Versions.Add(@($versionTrim,$VersionPath))
		}
	}
}

if ($DbList.Count -eq 0) {
	Write-Warning("В конфигурации не указаны строки подключения к БД 1С")
	$ParamIsGood = $false
}

if ($Versions.Count -eq 0) {
	Write-Warning("Не указаны версии обновления 1С")
	$ParamIsGood = $false
}

if (!$ParamIsGood) {
	Break
}

# Инициализация
$LogFileName = $WorkPath + "\" + (Get-Date).ToString("yyyyMMdd-HHmmss") + ".log"
$Exe1cv8 = $(if ($Debug) { "calc.exe" } else { $Exe1cv8 })

WriteLog "SANSoft(c) 2024. Обновление баз 1С"
WriteLog "=================================="
WriteLog
WriteLog "Режим отладки: $Debug"
WriteLog "Режим обновления: [$Mode] $ModeDescription"
WriteLog "Количество баз к обновлению: $($DbList.Count)"
ForEach ($DbKey In $DbList.Keys) {
	WriteLog "[$DbKey]"
}
WriteLog

WriteLog "Количество обновлений: $($Versions.Count)"
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

WriteConsole
WriteConsole "Итоги обновления:" -color ([ConsoleColor]::Cyan)
WriteConsole "=".PadRight($tableWidth,"=") -color ([ConsoleColor]::Cyan)
WriteConsole ($tableRowTemplate -f (ToLeftStringWithWidth "База" 15),(ToLeftStringWithWidth "Результат" 9), "Время") -color ([ConsoleColor]::Cyan) 
WriteConsole "=".PadRight($tableWidth,"=") -color ([ConsoleColor]::Cyan)
ForEach ($updResult In $UpdateResults) {
	WriteConsole $updResult -color ([ConsoleColor]::Cyan)
}
WriteConsole "=".PadRight($tableWidth,"=") -color ([ConsoleColor]::Cyan)

$span = ((Get-Date) - $overallStartTime).ToString()
WriteConsole "Общеее время обновления $span" -color ([ConsoleColor]::Cyan)

WriteConsole
WriteConsole
WriteConsole "SANSoft(c) 2024. Обновление баз 1С" -color ([ConsoleColor]::Blue)

ForceReleaseComConnection
