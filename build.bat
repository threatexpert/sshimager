@echo off
set app=sshimager

go mod tidy

set "LDFLAGS=-s -w -buildid="
set "BUILD_FLAGS=-buildvcs=false -trimpath"

echo Building Windows desktop application...
call wails build -clean -platform windows/amd64 -ldflags="%LDFLAGS%"
if errorlevel 1 exit /b 1
if not exist bin mkdir bin
copy /Y build\bin\%app%.exe bin\%app%.exe >nul

SET GOOS=windows
SET GOARCH=amd64
SET CGO_ENABLED=0
echo Building Windows CLI compatibility binary...
go build %BUILD_FLAGS% -tags cli -ldflags="%LDFLAGS%" -o bin/%app%-cli.exe
if errorlevel 1 exit /b 1

SET GOOS=linux
SET GOARCH=amd64
SET CGO_ENABLED=0
echo Building binary for %GOOS%_%GOARCH% ...
go build %BUILD_FLAGS% -ldflags="%LDFLAGS%" -o bin/%app%_%GOOS%_%GOARCH%

echo.
echo Building agent binaries...

pushd agent
call go mod tidy

SET GOOS=linux
SET GOARCH=amd64
SET CGO_ENABLED=0
echo Building agent for %GOOS%_%GOARCH% ...
go build %BUILD_FLAGS% -ldflags="%LDFLAGS%" -o ..\bin\agents\%GOOS%_%GOARCH%

SET GOOS=linux
SET GOARCH=arm64
SET CGO_ENABLED=0
echo Building agent for %GOOS%_%GOARCH% ...
go build %BUILD_FLAGS% -ldflags="%LDFLAGS%" -o ..\bin\agents\%GOOS%_%GOARCH%

SET GOOS=linux
SET GOARCH=386
SET CGO_ENABLED=0
echo Building agent for %GOOS%_%GOARCH% ...
go build %BUILD_FLAGS% -ldflags="%LDFLAGS%" -o ..\bin\agents\%GOOS%_%GOARCH%

popd

if not exist build\bin\agents mkdir build\bin\agents
xcopy /Y /Q bin\agents\* build\bin\agents\ >nul

echo.
echo Done.
dir bin\
dir bin\agents\
dir build\bin\
