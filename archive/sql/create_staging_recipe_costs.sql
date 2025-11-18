-- Create staging table for recipe costs
-- This table stores pre-calculated costs for combo/package items from APP_4_RECIPESUMMARY

IF OBJECT_ID('dbo.staging_recipe_costs', 'U') IS NOT NULL
    DROP TABLE dbo.staging_recipe_costs;
GO

CREATE TABLE dbo.staging_recipe_costs (
    item_id BIGINT NOT NULL,
    sales_type VARCHAR(10) NOT NULL,
    calculated_recipe_cost DECIMAL(18, 4) NOT NULL,
    ingredient_count INT NOT NULL,
    -- For tracking/debugging
    last_updated DATETIME DEFAULT GETDATE(),
    
    -- Composite primary key
    PRIMARY KEY (item_id, sales_type)
);
GO

-- Create index for fast lookups during transformation
CREATE INDEX IX_staging_recipe_costs_item_id 
ON dbo.staging_recipe_costs(item_id);
GO

PRINT 'Staging table for recipe costs created successfully!';
GO

