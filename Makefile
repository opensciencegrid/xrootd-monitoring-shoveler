VERSION=1.0.0
GO_FLAGS=CGO_ENABLED=0

all: bin/shoveler bin/createtoken

bin/shoveler: *.go
	$(GO_FLAGS) go build -ldflags "-X main.VERSION=$(VERSION)" -o bin/shoveler .
	$(GO_FLAGS) GOOS=linux GOARCH=amd64 go build -ldflags "-X main.VERSION=$(VERSION)" -o bin/linux-x64-shoveler .

bin/createtoken: cmd/createtoken/main.go
	$(GO_FLAGS) GOOS=linux GOARCH=amd64 go build -o bin/linux-x64-createtoken .
	$(GO_FLAGS) go build -o bin/createtoken ./cmd/createtoken