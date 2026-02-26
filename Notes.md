# Run these in local to start airflow server

  export AIRFLOW_HOME="/home/narutouzumaki/CursorProjects/yt_extract"
  export AIRFLOW__CORE__DAGS_FOLDER="/home/narutouzumaki/CursorProjects/yt_extract/dags"
  airflow standalone

# Add these to main docker file

    ENV AIRFLOW_HOME=/home/narutouzumaki/CursorProjects/yt_extract
    RUN airflow standalone
    RUN superset run -p 8088 --with-threads --reload --debugger --debug

# Add this to connect with duckdb

    duckdb:////app/data/yt_trending.duckdb