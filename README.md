# 📼 Cassette Tape

A real-time MySQL protocol query capture, analysis, and replay tool built with Go. Perfect for database performance testing, workload analysis, and debugging production query issues.

## 🚀 Features

- **Capture**: Real-time network packet capture for MySQL queries
- **Analyze**: Comprehensive query analysis and reporting using DuckDB
- **Replay**: Accurate query replay for testing and benchmarking
- **Cross-platform**: Support for Linux, macOS (ARM64/AMD64), and Windows

## 📋 Prerequisites

- Go 1.25 or higher
- libpcap development libraries
- Root/Administrator privileges (required for packet capture)

### Installing libpcap

**macOS:**
```bash
brew install libpcap
```

**Ubuntu/Debian:**
```bash
sudo apt-get install libpcap-dev
```

**CentOS/RHEL:**
```bash
sudo yum install libpcap-devel
```

## 🛠️ Building

### Using Makefile (Recommended)
```bash
make build
```

### Manual Build
```bash
CGO_ENABLED=1 go build -o cassette-tape main.go
```

## 📖 Usage

### Capture MySQL Queries

Capture queries from a specific network interface and port:

```bash
./cassette-tape capture --device lo0 --port 3306 --level info
```

**Options:**
- `--device`: Network interface (default: lo0)
- `--port`: MySQL port (default: 3306)
- `--level`: Log level - info or debug (default: info)

**Example:**
```bash
# Capture from loopback interface on port 3306
./cassette-tape capture --device lo0 --port 3306

# Capture with debug logging
./cassette-tape capture --device eth0 --port 3306 --level debug
```

### Analyze Captured Queries

Analyze the captured queries and generate reports:

```bash
./cassette-tape analyze --memory
```

**Options:**
- `--memory`: Enable DuckDB in-memory mode for faster processing

### Replay Queries

Replay captured queries against a target MySQL database:

```bash
./cassette-tape replay --host 127.0.0.1 --port 3306 --user root --password "" --db test --readonly --memory
```

**Options:**
- `--host`: Target MySQL host (default: 127.0.0.1)
- `--port`: Target MySQL port (default: 3306)
- `--user`: MySQL username (default: root)
- `--password`: MySQL password (default: "")
- `--db`: Target database name (default: test)
- `--readonly`: Only replay SELECT statements (default: true)
- `--memory`: Enable DuckDB in-memory mode (default: false)

**Example:**
```bash
# Replay queries to local MySQL in read-only mode
./cassette-tape replay --host localhost --user myuser --password mypass --db production --readonly

# Replay all query types (including INSERT/UPDATE/DELETE)
./cassette-tape replay --host 192.168.1.100 --user admin --password secret --db staging --readonly=false
```

## ⚠️ Important Notes & Limitations

### MySQL Client Configuration Requirements

When capturing queries, ensure your MySQL client has these settings:

```ini
# Disable prepared statements (required for parsing)
useServerPrepStmts=false

# Disable SSL (required for packet capture)
--ssl-mode=disabled
useSSL=false
```

### Packet Capture Limitations

- **Root/Admin Required**: Packet capture requires elevated privileges
- **Network Interface**: Must capture from the correct network interface
- **Port Filtering**: Only captures traffic on the specified port
- **TCP Only**: Currently supports only TCP connections

### Query Parsing Limitations

- **Prepare/Execute**: Cannot parse prepared statement prepare/execute pairs
- **SSL/TLS**: Encrypted connections cannot be captured
- **Compression**: Compressed connections may not be fully parsed

### Replay Limitations

- **Read-only Mode**: Default mode only replays SELECT statements
- **Transaction Handling**: Complex transactions may not replay correctly

## 🔧 Development

### Project Structure

```
cassette-tape/
├── analyze/          # Query analysis and reporting
├── capture/          # Network packet capture
├── db/              # Database connections (MySQL, DuckDB)
├── option/           # Configuration options
├── replay/           # Query replay engine
└── main.go          # CLI application entry point
```

### Building for Development

```bash
# Build and run
make dev

# Clean build artifacts
make clean

# Show available commands
make help
```

## 📝 Output Files

The capture command generates a JSON file with the following naming pattern:
```
Queries_YYYY-MM-DDTHH:MM:SS.json
```

This file contains all captured queries with metadata including:
- Source IP and port
- Query text
- Timestamp
- Connection information

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## ⚡ Performance Tips

- Use `--memory` flag for faster DuckDB operations
- Capture from loopback interface (lo0) for local testing
- Set appropriate log levels to reduce overhead
- Consider using dedicated network interfaces for production capture

## 🐛 Troubleshooting

### Common Issues

**Permission Denied:**
```bash
sudo ./cassette-tape capture
```

**Interface Not Found:**
```bash
# List available interfaces
ifconfig -a  # macOS/Linux
ip addr      # Linux
```

**No Queries Captured:**
- Verify MySQL client settings (disable SSL, prepared statements)
- Check network interface and port configuration
- Ensure MySQL traffic is flowing through the specified interface
- Verify root/admin privileges

**Build Errors:**
- Ensure CGO is enabled: `CGO_ENABLED=1`
- Install libpcap development libraries
- Use Go 1.25 or higher
