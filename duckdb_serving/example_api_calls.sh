#!/bin/bash
# Example API calls for DuckDB Server
# Run these commands to test the DuckDB server endpoints

# Set the API base URL
API_URL="${1:-http://localhost:8000}"

echo "DuckDB Server API Examples"
echo "================================"
echo "Base URL: $API_URL"
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Function to display command and execute
call_api() {
    local description=$1
    local method=$2
    local endpoint=$3
    local data=$4

    echo -e "${GREEN}► $description${NC}"
    echo -e "${BLUE}Command:${NC}"

    if [ "$method" == "GET" ]; then
        echo "curl $API_URL$endpoint"
        echo ""
        curl -s "$API_URL$endpoint" | python -m json.tool 2>/dev/null || echo "Error: Could not parse response"
    else
        echo "curl -X $method $API_URL$endpoint \\"
        echo "  -H 'Content-Type: application/json' \\"
        echo "  -d '$data'"
        echo ""
        curl -s -X "$method" "$API_URL$endpoint" \
            -H "Content-Type: application/json" \
            -d "$data" | python -m json.tool 2>/dev/null || echo "Error: Could not parse response"
    fi

    echo -e "\n---\n"
}

# 1. Health Check
call_api "1. Health Check" "GET" "/health" ""

# 2. Readiness Check
call_api "2. Readiness Check" "GET" "/ready" ""

# 3. Database Statistics
call_api "3. Database Statistics" "GET" "/stats" ""

# 4. List Tables
call_api "4. List All Tables" "GET" "/tables" ""

# 5. Get Schema for youtube_categories
call_api "5. Get Schema for youtube_categories" "GET" "/schema/youtube_categories" ""

# 6. Query Categories
call_api "6. Query YouTube Categories" "POST" "/query" \
    '{"sql": "SELECT * FROM youtube_categories LIMIT 10"}'

# 7. Count Categories
call_api "7. Count Categories" "POST" "/query" \
    '{"sql": "SELECT COUNT(*) as total_categories FROM youtube_categories"}'

# 8. Create a Test Table
call_api "8. Create Test Table" "POST" "/execute" \
    '{"sql": "CREATE TABLE IF NOT EXISTS test_data (id INTEGER PRIMARY KEY, value VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"}'

# 9. Insert Test Data
call_api "9. Insert Test Data" "POST" "/execute" \
    '{"sql": "INSERT INTO test_data (id, value) VALUES (1, '\''test_value_1'\'')"}'

# 10. Query Test Data
call_api "10. Query Test Data" "POST" "/query" \
    '{"sql": "SELECT * FROM test_data"}'

# 11. Complex Query - Trending Summary View
call_api "11. Query Trending Summary View" "POST" "/query" \
    '{"sql": "SELECT * FROM information_schema.tables WHERE table_schema = '\''main'\''"}'

# 12. Get Table Count
call_api "12. Get Table Count" "POST" "/query" \
    '{"sql": "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = '\''main'\''"}'

echo "================================"
echo "API Examples Complete!"
echo ""
echo "For interactive API documentation, visit:"
echo "  $API_URL/docs"
echo ""
echo "For ReDoc documentation, visit:"
echo "  $API_URL/redoc"

