all: clean build

clean:
	@echo "Cleaning up..."
	@go mod tidy -v
	@rm -rf ./bin

build:
	@echo "Building bin/example..."
	@go build -o bin/example ./example

test:
	@echo "Running tests..."
	@go test -race -v -cover ./...
