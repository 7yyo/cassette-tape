# Makefile for insight project

# Go related variables
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean

# Project name
BINARY_NAME=cassette-tape

# Build flags for better compatibility
LDFLAGS=-ldflags="-s -w"

# Auto-detect architecture and OS
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Default target
all: build

# Build project (current platform)
build:
	@echo "🏗️ Architecture: $(UNAME_S) $(UNAME_M)"
	@echo "🔨 Building project for current platform..."
	CGO_ENABLED=1 $(GOBUILD) -o $(BINARY_NAME) main.go
	@echo "✅ Build completed"

# Clean build files
clean:
	@echo "🧹 Cleaning build files..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME) $(BINARY_NAME)-*
	@echo "✅ Clean completed"

# Run project
run: build
	@echo "🚀 Running $(BINARY_NAME)..."
	./$(BINARY_NAME)

# Development mode
dev: build run

# Help
help:
	@echo "📋 Available commands:"
	@echo "  build       - Build for current platform"
	@echo "  clean       - Clean build files"
	@echo "  run         - Build and run the project"
	@echo "  dev         - Build and run (development mode)"

.PHONY: all build clean run dev help


