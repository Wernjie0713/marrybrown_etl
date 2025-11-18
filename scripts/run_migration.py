"""
SQL Migration Runner for MarryBrown ETL
Runs SQL migration files against the warehouse database

Author: YONG WERN JIE
Date: November 7, 2025
"""

import os
import sys
import pyodbc
from dotenv import load_dotenv
from pathlib import Path
import re

def load_environment():
    """Load environment variables, preferring local override when available.

    Priority:
    1. .env.local at repo root (for local development)
    2. .env.cloud next to this script (for cloud / shared env)
    """
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent

    # Prefer local env for development
    local_env_path = repo_root / '.env.local'
    cloud_env_path = script_dir / '.env.cloud'

    if local_env_path.exists():
        load_dotenv(local_env_path)
        print(f"[INFO] Loaded environment from: {local_env_path}")
        return

    if cloud_env_path.exists():
        load_dotenv(cloud_env_path)
        print(f"[INFO] Loaded environment from: {cloud_env_path}")
        return

    # If neither exists, fail fast with clear message
    print("[ERROR] No environment file found for migration runner.")
    print(f"  Checked: {local_env_path}")
    print(f"  Checked: {cloud_env_path}")
    sys.exit(1)

def get_warehouse_connection():
    """Create connection to warehouse database"""
    server = os.getenv('TARGET_SERVER')
    database = os.getenv('TARGET_DATABASE')
    username = os.getenv('TARGET_USERNAME')
    password = os.getenv('TARGET_PASSWORD')
    
    if not all([server, database, username, password]):
        print("[ERROR] Missing warehouse credentials in .env.cloud")
        print("Required: TARGET_SERVER, TARGET_DATABASE, TARGET_USERNAME, TARGET_PASSWORD")
        sys.exit(1)
    
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    
    try:
        conn = pyodbc.connect(connection_string, timeout=30)
        print(f"[SUCCESS] Connected to warehouse: {database} on {server}")
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to warehouse: {e}")
        sys.exit(1)

def read_sql_file(file_path):
    """Read SQL file content"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        print(f"[ERROR] Failed to read SQL file: {e}")
        return None

def split_sql_batches(sql_content):
    """
    Split SQL content by GO statements
    Returns list of SQL batches to execute separately
    """
    # Split by GO statement (case insensitive, must be on its own line)
    batches = re.split(r'^\s*GO\s*$', sql_content, flags=re.MULTILINE | re.IGNORECASE)
    
    # Clean up batches (remove empty ones and strip whitespace)
    batches = [batch.strip() for batch in batches if batch.strip()]
    
    return batches

def execute_migration(conn, sql_content, file_name):
    """Execute SQL migration file"""
    print()
    print("="*80)
    print(f"  Executing Migration: {file_name}")
    print("="*80)
    print()
    
    # Split into batches (in case of GO statements)
    batches = split_sql_batches(sql_content)
    
    print(f"[INFO] Found {len(batches)} SQL batch(es) to execute")
    print()
    
    cursor = conn.cursor()
    
    try:
        for i, batch in enumerate(batches, 1):
            if not batch.strip():
                continue
                
            print(f"[BATCH {i}/{len(batches)}] Executing...")
            
            # Show first 100 chars of the batch for context
            preview = batch[:100].replace('\n', ' ')
            if len(batch) > 100:
                preview += "..."
            print(f"  SQL: {preview}")
            
            # Execute batch
            cursor.execute(batch)
            
            # Get row count if applicable
            if cursor.rowcount >= 0:
                print(f"  [OK] Affected {cursor.rowcount} row(s)")
            else:
                print(f"  [OK] Executed successfully")
            
            print()
        
        # Commit all changes
        conn.commit()
        print(f"[SUCCESS] Migration completed: {file_name}")
        print()
        return True
        
    except Exception as e:
        print()
        print(f"[ERROR] Migration failed: {e}")
        print()
        conn.rollback()
        return False
    finally:
        cursor.close()

def list_migrations():
    """List all available migration files"""
    # Migrations live in the repo root under 'migrations', not inside 'scripts'
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    migrations_dir = repo_root / 'migrations'
    
    if not migrations_dir.exists():
        print(f"[ERROR] Migrations directory not found: {migrations_dir}")
        return []
    
    # Find all .sql files
    sql_files = sorted(migrations_dir.glob('*.sql'))
    
    return sql_files

def run_migration(file_path=None, migration_name=None):
    """
    Run a specific migration file
    
    Args:
        file_path: Full path to migration file
        migration_name: Name of migration file (e.g., '001_create_etl_progress_table.sql')
    """
    # Determine migration file
    if file_path:
        migration_path = Path(file_path)
    elif migration_name:
        # Resolve from repo-root migrations folder
        script_dir = Path(__file__).resolve().parent
        repo_root = script_dir.parent
        migration_path = repo_root / 'migrations' / migration_name
    else:
        print("[ERROR] Must provide either file_path or migration_name")
        sys.exit(1)
    
    if not migration_path.exists():
        print(f"[ERROR] Migration file not found: {migration_path}")
        sys.exit(1)
    
    print()
    print("="*80)
    print(" "*25 + "SQL MIGRATION RUNNER")
    print("="*80)
    print()
    print(f"  Migration File: {migration_path.name}")
    print(f"  Full Path: {migration_path}")
    print()
    
    # Load environment
    load_environment()
    print()
    
    # Read SQL file
    print(f"[INFO] Reading SQL file...")
    sql_content = read_sql_file(migration_path)
    
    if not sql_content:
        print("[ERROR] SQL file is empty or could not be read")
        sys.exit(1)
    
    print(f"[INFO] SQL file size: {len(sql_content)} characters")
    
    # Confirmation
    print()
    print("This will execute the SQL migration against your warehouse database.")
    print()
    print("[INFO] Starting in 3 seconds...")
    print("[INFO] Press Ctrl+C to cancel...")
    print()
    
    import time
    try:
        for i in range(3, 0, -1):
            print(f"  {i}...")
            time.sleep(1)
    except KeyboardInterrupt:
        print()
        print("[CANCELLED] User interrupted")
        sys.exit(0)
    
    print()
    
    # Connect to warehouse
    conn = get_warehouse_connection()
    print()
    
    # Execute migration
    success = execute_migration(conn, sql_content, migration_path.name)
    
    # Close connection
    conn.close()
    print("[INFO] Database connection closed")
    print()
    
    if success:
        print("="*80)
        print(" "*25 + "MIGRATION SUCCESSFUL")
        print("="*80)
        print()
        return True
    else:
        print("="*80)
        print(" "*25 + "MIGRATION FAILED")
        print("="*80)
        print()
        return False

def run_all_migrations():
    """Run all migrations in order"""
    print()
    print("="*80)
    print(" "*20 + "RUN ALL SQL MIGRATIONS")
    print("="*80)
    print()
    
    # List migrations
    migrations = list_migrations()
    
    if not migrations:
        print("[ERROR] No migration files found in migrations/ directory")
        sys.exit(1)
    
    print(f"[INFO] Found {len(migrations)} migration(s):")
    for i, migration in enumerate(migrations, 1):
        print(f"  {i}. {migration.name}")
    print()
    
    # Confirmation
    print("This will execute ALL migrations in order against your warehouse.")
    print()
    print("[INFO] Press Enter to continue or Ctrl+C to cancel...")
    try:
        input()
    except KeyboardInterrupt:
        print()
        print("[CANCELLED] User interrupted")
        sys.exit(0)
    
    # Load environment
    load_environment()
    print()
    
    # Connect to warehouse
    conn = get_warehouse_connection()
    print()
    
    # Run each migration
    successful = 0
    failed = 0
    
    for migration in migrations:
        sql_content = read_sql_file(migration)
        if not sql_content:
            print(f"[SKIP] Could not read: {migration.name}")
            failed += 1
            continue
        
        success = execute_migration(conn, sql_content, migration.name)
        
        if success:
            successful += 1
        else:
            failed += 1
            print(f"[ERROR] Migration failed, stopping execution: {migration.name}")
            break
    
    # Close connection
    conn.close()
    print("[INFO] Database connection closed")
    print()
    
    # Summary
    print("="*80)
    print(" "*25 + "MIGRATION SUMMARY")
    print("="*80)
    print()
    print(f"  Total Migrations: {len(migrations)}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print()
    
    if failed == 0:
        print("  ✓ All migrations completed successfully!")
        print()
        return True
    else:
        print("  ✗ Some migrations failed")
        print()
        return False

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print()
        print("="*80)
        print(" "*25 + "SQL MIGRATION RUNNER")
        print("="*80)
        print()
        print("Usage:")
        print()
        print("  Run specific migration:")
        print("    python run_migration.py <migration_file.sql>")
        print()
        print("  Run all migrations:")
        print("    python run_migration.py all")
        print()
        print("Examples:")
        print("    python run_migration.py 001_create_etl_progress_table.sql")
        print("    python run_migration.py all")
        print()
        print("Available migrations:")
        
        migrations = list_migrations()
        if migrations:
            for i, migration in enumerate(migrations, 1):
                print(f"  {i}. {migration.name}")
        else:
            print("  (No migration files found)")
        print()
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command.lower() == 'all':
        # Run all migrations
        success = run_all_migrations()
    else:
        # Run specific migration
        success = run_migration(migration_name=command)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

