#!/bin/bash

# Script to restore PostgreSQL database from backup
# Usage: ./restore_backup.sh <backup_file> <postgres_conn_str>

set -e

BACKUP_FILE="${1:-backups/dump_20251219_020014}"
POSTGRES_CONN_STR="${2:-$POSTGRES_CONN_STR}"

if [ -z "$POSTGRES_CONN_STR" ]; then
    echo "Error: PostgreSQL connection string not provided"
    echo "Usage: $0 [backup_file] [postgres_conn_str]"
    echo "Or set POSTGRES_CONN_STR environment variable"
    exit 1
fi

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Error: Backup file '$BACKUP_FILE' not found"
    exit 1
fi

echo "Backup file: $BACKUP_FILE"
echo "Restoring database..."

# Use pg_restore to restore the custom format dump
# -c: Clean (drop) database objects before recreating them
# -d: Specify database connection string
# -v: Verbose mode
pg_restore -c -v -d "$POSTGRES_CONN_STR" "$BACKUP_FILE"

echo "Database restored successfully!"
