"""
Create Users Table in Cloud Warehouse

This script creates the users table for authentication and populates it with
example users. Required for portal login functionality.

Author: YONG WERN JIE
Date: November 3, 2025
"""

import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime

def get_db_engine():
    """Creates a SQLAlchemy engine from .env.cloud credentials."""
    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER")
    database = os.getenv("TARGET_DATABASE")
    user = os.getenv("TARGET_USERNAME")
    password = quote_plus(os.getenv("TARGET_PASSWORD"))
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes"
    )
    
    return create_engine(connection_uri)


def create_users_table(engine):
    """Create the users table if it doesn't exist."""
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users' AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        CREATE TABLE dbo.users (
            id INT IDENTITY(1,1) PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            hashed_password VARCHAR(255) NOT NULL,
            full_name VARCHAR(255) NOT NULL,
            is_active BIT NOT NULL DEFAULT 1,
            is_superuser BIT NOT NULL DEFAULT 0,
            created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
            updated_at DATETIME2 NOT NULL DEFAULT GETDATE()
        );
        
        -- Create index on email for faster lookups
        CREATE INDEX idx_users_email ON dbo.users(email);
        
        PRINT 'Users table created successfully.';
    END
    ELSE
    BEGIN
        PRINT 'Users table already exists.';
    END
    """
    
    with engine.connect() as connection:
        connection.execute(text(create_table_sql))
        connection.commit()
        print("✅ Users table check/creation completed.")


def insert_users(engine):
    """Insert example users into the users table."""
    # Example users data
    users = [
        {
            "email": "admin@marrybrown.com",
            "hashed_password": "$2b$12$.1CTmxKI5o6q7uncgn.S/.4p7DL5kqd5tY7RKP2giX3Q5I9L1TNZK",
            "full_name": "Admin User",
            "is_active": True,
            "is_superuser": True,
            "created_at": datetime(2025, 10, 7, 16, 55, 24),
            "updated_at": datetime(2025, 10, 7, 16, 55, 24)
        },
        {
            "email": "user@example.com",
            "hashed_password": "$2b$12$0RqHHaxOEju21bgCGh7JQuvseohxq1AFGer5k2qkbd0Z90aPYsTDa",
            "full_name": "user",
            "is_active": True,
            "is_superuser": False,
            "created_at": datetime(2025, 10, 7, 17, 26, 34),
            "updated_at": datetime(2025, 10, 7, 17, 26, 34)
        },
        {
            "email": "test@marrybrown.com",
            "hashed_password": "$2b$12$MQzaSZBdo4pEwf8CAL5Ck.KFAhRbvxLDmFAAQHYT5AfdGVYvf4l26",
            "full_name": "Test User",
            "is_active": True,
            "is_superuser": False,
            "created_at": datetime(2025, 10, 14, 9, 30, 27),
            "updated_at": datetime(2025, 10, 14, 9, 30, 27)
        }
    ]
    
    with engine.connect() as connection:
        # Check if users already exist
        check_sql = text("SELECT COUNT(*) as count FROM dbo.users")
        result = connection.execute(check_sql)
        existing_count = result.scalar()
        
        if existing_count > 0:
            print(f"⚠️  Users table already contains {existing_count} user(s).")
            response = input("Do you want to insert example users anyway? (y/n): ").strip().lower()
            if response != 'y':
                print("Skipping user insertion.")
                return
        
        # Insert users one by one (handling duplicates)
        inserted_count = 0
        skipped_count = 0
        
        for user in users:
            try:
                insert_sql = text("""
                    IF NOT EXISTS (SELECT 1 FROM dbo.users WHERE email = :email)
                    BEGIN
                        INSERT INTO dbo.users (email, hashed_password, full_name, is_active, is_superuser, created_at, updated_at)
                        VALUES (:email, :hashed_password, :full_name, :is_active, :is_superuser, :created_at, :updated_at);
                    END
                """)
                
                connection.execute(
                    insert_sql,
                    {
                        "email": user["email"],
                        "hashed_password": user["hashed_password"],
                        "full_name": user["full_name"],
                        "is_active": user["is_active"],
                        "is_superuser": user["is_superuser"],
                        "created_at": user["created_at"],
                        "updated_at": user["updated_at"]
                    }
                )
                connection.commit()
                inserted_count += 1
                print(f"  ✅ Inserted: {user['email']} ({'Admin' if user['is_superuser'] else 'User'})")
            except Exception as e:
                skipped_count += 1
                print(f"  ⚠️  Skipped {user['email']}: {str(e)}")
        
        print(f"\n✅ User insertion completed: {inserted_count} inserted, {skipped_count} skipped.")


def verify_users(engine):
    """Verify the users were created successfully."""
    verify_sql = text("""
        SELECT 
            id,
            email,
            full_name,
            is_active,
            is_superuser,
            created_at,
            updated_at
        FROM dbo.users
        ORDER BY id
    """)
    
    with engine.connect() as connection:
        result = connection.execute(verify_sql)
        users = result.fetchall()
        
        if users:
            print("\n" + "="*80)
            print("VERIFIED USERS IN DATABASE")
            print("="*80)
            print(f"{'ID':<5} {'Email':<30} {'Full Name':<20} {'Active':<8} {'Superuser':<10}")
            print("-"*80)
            for user in users:
                print(f"{user.id:<5} {user.email:<30} {user.full_name:<20} {'Yes' if user.is_active else 'No':<8} {'Yes' if user.is_superuser else 'No':<10}")
            print("="*80)
        else:
            print("⚠️  No users found in database.")


def main():
    """Main function to create users table and populate with example data."""
    print("="*80)
    print("CREATE USERS TABLE FOR CLOUD WAREHOUSE")
    print("="*80)
    print()
    
    try:
        # Load cloud environment variables
        load_dotenv('.env.cloud')
        
        # Verify required environment variables
        required_vars = ['TARGET_SERVER', 'TARGET_DATABASE', 'TARGET_USERNAME', 'TARGET_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
            print("Please ensure .env.cloud file exists and contains all TARGET_* variables.")
            return
        
        print("✅ Environment variables loaded.")
        print(f"   Server: {os.getenv('TARGET_SERVER')}")
        print(f"   Database: {os.getenv('TARGET_DATABASE')}")
        print()
        
        # Create database engine
        print("Connecting to cloud database...")
        engine = get_db_engine()
        print("✅ Connected successfully.")
        print()
        
        # Create users table
        print("Creating users table...")
        create_users_table(engine)
        print()
        
        # Insert example users
        print("Inserting example users...")
        insert_users(engine)
        print()
        
        # Verify users
        verify_users(engine)
        print()
        
        print("✅ All operations completed successfully!")
        print("\nYou can now log in to the portal using:")
        print("  - Admin: admin@marrybrown.com")
        print("  - User: user@example.com")
        print("  - Test: test@marrybrown.com")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

