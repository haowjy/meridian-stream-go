.PHONY: examples clean test run-simple run-nethttp-sse run-catchup-test run-catchup-db-test run-persist-race-test

# Build all example binaries
examples:
	@echo "Building all examples..."
	@cd examples/simple && go build -o ../../simple
	@cd examples/nethttp-sse && go build -o ../../nethttp-sse
	@cd examples/catchup-test && go build -o ../../catchup-test
	@cd examples/catchup-db-test && go build -o ../../catchup-db-test
	@cd examples/persist-race-test && go build -o ../../persist-race-test
	@echo "Examples built successfully!"

# Clean example binaries
clean:
	@echo "Cleaning example binaries..."
	@rm -f simple nethttp-sse catchup-test catchup-db-test persist-race-test
	@echo "Cleaned!"

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run individual examples
run-simple: examples
	@./simple

run-nethttp-sse: examples
	@./nethttp-sse

run-catchup-test: examples
	@./catchup-test

run-catchup-db-test: examples
	@./catchup-db-test

run-persist-race-test: examples
	@./persist-race-test

# Help
help:
	@echo "Available targets:"
	@echo "  make examples               - Build all example binaries"
	@echo "  make clean                  - Remove all example binaries"
	@echo "  make test                   - Run all tests"
	@echo "  make run-simple             - Build and run simple example"
	@echo "  make run-nethttp-sse        - Build and run net/http SSE example"
	@echo "  make run-catchup-test       - Build and run catchup test"
	@echo "  make run-catchup-db-test    - Build and run catchup DB test"
	@echo "  make run-persist-race-test  - Build and run persist race test"
