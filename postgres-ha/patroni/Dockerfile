FROM postgres:16

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        jq \
        python3 \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/patroni \
    && /opt/patroni/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
    && /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3,psycopg3]" psycopg2-binary

ENV PATH="/opt/patroni/bin:${PATH}"

COPY patroni.yml.template /etc/patroni/patroni.yml.template
COPY entrypoint.sh /usr/local/bin/patroni-entrypoint.sh
COPY post_init.sh /usr/local/bin/patroni-post-init.sh

RUN chmod +x /usr/local/bin/patroni-entrypoint.sh /usr/local/bin/patroni-post-init.sh

ENTRYPOINT ["patroni-entrypoint.sh"]
