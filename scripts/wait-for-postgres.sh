#!/bin/sh
# wait-for-postgres.sh
# Source: https://gist.github.com/zhashkevych/2742682ab57b5670a15291864658625b
# Own adaptations to use POSTGRES environment variables 

set -e

cmd="$@"

until PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up - executing command"
exec $cmd
