FROM gvenzl/oracle-free:23-slim

COPY init/ /container-entrypoint-initdb.d/

USER root
RUN find /container-entrypoint-initdb.d -type f -name "*.sh" -exec chmod 755 {} \;
USER oracle
