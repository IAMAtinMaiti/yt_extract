"""
DuckDB Database Initialization Script
Creates initial schema and tables for YouTube trending data
"""

import os
import sys
import logging
import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Database path
DB_PATH = os.getenv("DUCKDB_DATA_PATH", "/data/duckdb/data.duckdb")

# Ensure data directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def init_database():
    """Initialize DuckDB with schema and tables"""
    try:
        conn = duckdb.connect(DB_PATH)
        logger.info(f"Connected to DuckDB at {DB_PATH}")

        # Create youtube_videos table
        conn.execute("""
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
            )
        """)
        logger.info("Created youtube_videos table")

        # Create youtube_categories table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS youtube_categories (
                category_id INTEGER PRIMARY KEY,
                category_name VARCHAR,
                snippet_title VARCHAR
            )
        """)
        logger.info("Created youtube_categories table")

        # Create indices for better query performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel ON youtube_videos(channel_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_category ON youtube_videos(category_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_publish_time ON youtube_videos(publish_time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_snapshot_date ON youtube_videos(snapshot_date)")
        logger.info("Created indices")

        # Create materialized view for trending summary
        conn.execute("""
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
            GROUP BY snapshot_date, category_id
        """)
        logger.info("Created trending_summary view")

        # Insert sample category data
        categories = [
            (1, "Film & Animation", "Film & Animation"),
            (2, "Autos & Vehicles", "Autos & Vehicles"),
            (10, "Music", "Music"),
            (15, "Pets & Animals", "Pets & Animals"),
            (17, "Sports", "Sports"),
            (18, "Short Movies", "Short Movies"),
            (19, "Travel & Events", "Travel & Events"),
            (20, "Gaming", "Gaming"),
            (21, "Videoblogging", "Videoblogging"),
            (22, "People & Blogs", "People & Blogs"),
            (23, "Comedy", "Comedy"),
            (24, "Entertainment", "Entertainment"),
            (25, "News & Politics", "News & Politics"),
            (26, "Howto & Style", "Howto & Style"),
            (27, "Education", "Education"),
            (28, "Science & Technology", "Science & Technology"),
            (29, "Nonprofits & Activism", "Nonprofits & Activism"),
            (30, "Movies", "Movies"),
            (31, "Anime/Animation", "Anime/Animation"),
            (32, "Action/Adventure", "Action/Adventure"),
            (33, "Classics", "Classics"),
            (34, "Comedies", "Comedies"),
            (35, "Documentaries", "Documentaries"),
            (36, "Dramas", "Dramas"),
            (37, "Family", "Family"),
            (38, "Foreign", "Foreign"),
            (39, "Horror", "Horror"),
            (40, "Sci-Fi/Fantasy", "Sci-Fi/Fantasy"),
            (41, "Thrillers", "Thrillers"),
            (42, "Shorts", "Shorts"),
            (43, "Shows", "Shows"),
            (44, "Trailers", "Trailers"),
        ]

        for cat_id, cat_name, snippet_title in categories:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO youtube_categories 
                    (category_id, category_name, snippet_title) 
                    VALUES (?, ?, ?)
                """, [cat_id, cat_name, snippet_title])
            except:
                pass  # Ignore duplicates

        logger.info(f"Inserted {len(categories)} category records")

        # Optimize database
        conn.execute("VACUUM")
        logger.info("Database optimization completed")

        conn.close()
        logger.info("Database initialization completed successfully")
        return True

    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = init_database()
    sys.exit(0 if success else 1)

