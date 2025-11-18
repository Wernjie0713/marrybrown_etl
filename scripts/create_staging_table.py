"""
Create the staging_recipe_costs table in the warehouse database.
Run this ONCE before running the ETL scripts.
"""
from sqlalchemy import create_engine, text
from database import get_db_engine

def create_staging_recipe_costs_table():
    """Create the staging_recipe_costs table if it doesn't exist"""
    
    print("Creating staging_recipe_costs table...")
    
    engine = get_db_engine("TARGET")
    
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[staging_recipe_costs]') AND type in (N'U'))
    BEGIN
        CREATE TABLE dbo.staging_recipe_costs (
            item_id BIGINT NOT NULL,
            sales_type VARCHAR(50) NOT NULL,
            calculated_recipe_cost DECIMAL(18, 4) NOT NULL,
            ingredient_count INT NOT NULL,
            -- Add a primary key for efficient lookups
            CONSTRAINT PK_staging_recipe_costs PRIMARY KEY (item_id, sales_type)
        );
        PRINT 'Table created successfully';
    END
    ELSE
    BEGIN
        PRINT 'Table already exists';
    END
    """
    
    try:
        with engine.connect() as connection:
            connection.execute(text(create_table_sql))
            connection.commit()
        print("✅ staging_recipe_costs table is ready")
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        raise
    
    finally:
        engine.dispose()

if __name__ == "__main__":
    create_staging_recipe_costs_table()

