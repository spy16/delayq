all: clean build

clean:
	@echo "Cleaning up..."
	@go mod tidy -v
	@rm -rf ./bin

build:
	@echo "Building bin/reminder..."
	@go build -o bin/reminder ./reminder

install:
	@echo "Installing to GOBIN..."
	@go install ./reminder
