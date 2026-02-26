
# Add these to main docker file

    ENV AIRFLOW_HOME=/home/narutouzumaki/CursorProjects/yt_extract
    RUN airflow standalone
    RUN superset run -p 8088 --with-threads --reload --debugger --debug