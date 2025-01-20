#!/bin/bash
# scripts/setup_redis.sh

# Check if Redis is already installed
if ! command -v redis-cli &> /dev/null; then
    echo "Redis not found. Installing Redis..."
    
    # Check OS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        brew install redis
        brew services start redis
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        sudo apt-get update
        sudo apt-get install -y redis-server
        sudo systemctl enable redis-server
        sudo systemctl start redis-server
    else
        echo "Unsupported operating system"
        exit 1
    fi
else
    echo "Redis already installed"
fi

# Test Redis connection
if redis-cli ping > /dev/null 2>&1; then
    echo "Redis is running"
else
    echo "Starting Redis..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew services start redis
    else
        sudo systemctl start redis-server
    fi
fi

# Clear any existing data
echo "Clearing existing Redis data..."
redis-cli FLUSHALL

# Set optimal Redis configuration
echo "Configuring Redis..."
redis-cli CONFIG SET maxmemory 512mb
redis-cli CONFIG SET maxmemory-policy allkeys-lru
redis-cli CONFIG SET appendonly yes

echo "Redis setup complete!"