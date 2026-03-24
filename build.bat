@echo off
set app=sshimager

go mod tidy

set "LDFLAGS=-s -w -buildid="
set "BUILD_FLAGS=-buildvcs=false -trimpath"

SET GOOS=windows
SET GOARCH=amd64
SET CGO_ENABLED=0
echo Building binary for %GOOS%_%GOARCH% ...
go build %BUILD_FLAGS% -ldflags="%LDFLAGS%" -o bin/%app%.exe

SET GOOS=linux
SET GOARCH=amd64
SET CGO_ENABLED=0
echo Building binary for %GOOS%_%GOARCH% ...
go build %BUILD_FLAGS% -ldflags="%LDFLAGS%" -o bin/%app%_%GOOS%_%GOARCH%

echo Done.
dir bin\
