build:
	GOARCH=amd64 GOOS=linux go build -o gunzipper

.PHONY: build