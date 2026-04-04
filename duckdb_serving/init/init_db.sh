#!/bin/bash
# DuckDB Initialization Script
# Creates initial schema and sample data

echo "Initializing DuckDB..."

DB_PATH="${DUCKDB_DATA_PATH:-/data/duckdb/data.duckdb}"

# Create initialization SQL script
cat > /tmp/init.sql << 'EOF'
-- Create sample tables for YouTube trending data

CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    channel_id VARCHAR,
    channel_title VARCHAR,
    category_id INTEGER,
    publish_time TIMESTAMP,
    tags VARCHAR,
    views INTEGER,
    likes INTEGER,
    comment_count INTEGER,
    thumbnail_link VARCHAR,
    comments_disabled BOOLEAN,
    ratings_disabled BOOLEAN,
    snapshot_date DATE
);

CREATE TABLE IF NOT EXISTS youtube_categories (
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR,
    snippet_title VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_videos_channel ON youtube_videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_videos_category ON youtube_videos(category_id);
CREATE INDEX IF NOT EXISTS idx_videos_publish_time ON youtube_videos(publish_time);
CREATE INDEX IF NOT EXISTS idx_videos_snapshot_date ON youtube_videos(snapshot_date);

-- Create materialized view for trending summary
CREATE VIEW IF NOT EXISTS trending_summary AS
SELECT
    snapshot_date,
    category_id,
    COUNT(*) as video_count,
    AVG(views) as avg_views,
    AVG(likes) as avg_likes,
    AVG(comment_count) as avg_comments,
    MAX(views) as max_views,
    MIN(views) as min_views
FROM youtube_videos
GROUP BY snapshot_date, category_id;

VACUUM;
EOF

# Execute the initialization script
duckdb "$DB_PATH" < /tmp/init.sql

if [ $? -eq 0 ]; then
    echo "DuckDB initialization completed successfully"
    rm /tmp/init.sql
else
    echo "DuckDB initialization failed"
    exit 1
fi

