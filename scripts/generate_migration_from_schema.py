"""
Generate migration SQL from actual Xilnex schema.
This ensures we replicate exactly what exists in Xilnex, not what we think should be there.
"""
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
SCHEMA_FILE = PROJECT_ROOT / "docs" / "xilnex_full_schema.json"
REPLICA_SCHEMA = PROJECT_ROOT / "docs" / "replica_schema.json"
OUTPUT_FILE = PROJECT_ROOT / "migrations" / "schema_tables" / "100_create_replica_tables.sql"

# Tables to replicate (from replica_schema.json)
replica_data = json.loads(REPLICA_SCHEMA.read_text(encoding="utf-8"))
table_names = [t["name"] for t in replica_data["tables"]]

# Load full Xilnex schema
full_schema = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))

# Columns that exceed INT range and need promotion to BIGINT or DECIMAL(38,0)
# Format: (table_name, column_name) -> promoted_type
INT_OVERFLOW_PROMOTIONS = {
    # APP_4_ITEM: PAIR_ID can exceed INT max (14,950,152,321 > 2,147,483,647)
    ("APP_4_ITEM", "PAIR_ID"): "BIGINT",
    
    # APP_4_STOCK: These DECIMAL columns contain values exceeding DECIMAL(16,4) range
    # Values like 472,257,242,547 exceed DECIMAL(16,4) max
    # Already handled by DECIMAL(38,20) default, but ensure they stay as DECIMAL
    # (No promotion needed - already DECIMAL in source)
    
    # APP_4_VOUCHER_MASTER: INT columns that can exceed INT max (2,000,000,000)
    ("APP_4_VOUCHER_MASTER", "CAMPAIGN_TARGET"): "BIGINT",
    ("APP_4_VOUCHER_MASTER", "MAX_REDEMPTION_PER_CLIENT"): "BIGINT",
    ("APP_4_VOUCHER_MASTER", "MAX_REDEMPTION_PER_CAMPAIGN"): "BIGINT",
    
    # APP_4_POINTRECORD: DOCUMENT_ID can exceed INT max (90,011,539,761 > 2,147,483,647)
    # Source is already BIGINT, but ensure it stays BIGINT
    # (No promotion needed - already BIGINT in source)
    
    # APP_4_CASHIER_DRAWER: INT_EXTEND_2 can have large values (5,319,464 is within INT range)
    # But to be safe for future growth, we could promote, but current values are OK
    # Leaving as-is for now
}


def sql_type_from_schema(col, table_name=None):
    """Convert schema type to SQL Server type."""
    col_name = col["name"]
    col_type = col["type"].upper()
    char_len = col.get("char_len")
    numeric_precision = col.get("numeric_precision")
    numeric_scale = col.get("numeric_scale")
    
    # Check if this column needs promotion due to INT overflow
    if table_name and (table_name, col_name) in INT_OVERFLOW_PROMOTIONS:
        promoted_type = INT_OVERFLOW_PROMOTIONS[(table_name, col_name)]
        print(f"[PROMOTE] {table_name}.{col_name}: {col_type} -> {promoted_type} (INT overflow protection)")
        return promoted_type
    
    if col_type in ("VARCHAR", "NVARCHAR", "CHAR", "NCHAR"):
        # char_len: -1 means MAX in SQL Server INFORMATION_SCHEMA
        if char_len == -1 or (char_len is None):
            return f"{col_type}(MAX)"
        elif char_len and char_len > 0:
            return f"{col_type}({char_len})"
        else:
            return f"{col_type}(MAX)"
    elif col_type == "DECIMAL":
        # Use maximum DECIMAL size to handle any value from source
        # DECIMAL(38,20) = 18 integer digits + 20 decimal places (safe for most cases)
        # This ensures 1:1 replication without precision issues
        return "DECIMAL(38,20)"
    elif col_type == "NUMERIC":
        # Use maximum NUMERIC size to handle any value from source
        # NUMERIC(38,20) = 18 integer digits + 20 decimal places (safe for most cases)
        # This ensures 1:1 replication without precision issues
        return "NUMERIC(38,20)"
    elif col_type in ("BIGINT", "INT", "SMALLINT", "TINYINT", "BIT"):
        return col_type
    elif col_type in ("DATE", "DATETIME", "DATETIME2", "TIME", "DATETIMEOFFSET"):
        return col_type
    elif col_type == "TIMESTAMP":
        # Use VARBINARY(8) instead of ROWVERSION to allow 1:1 replication
        # ROWVERSION is auto-generated and cannot be inserted, but we need to preserve exact values
        return "VARBINARY(8)"
    elif col_type == "TEXT":
        return "NVARCHAR(MAX)"
    elif col_type == "NTEXT":
        return "NVARCHAR(MAX)"
    elif col_type == "IMAGE":
        return "VARBINARY(MAX)"
    else:
        return col_type


def generate_table_sql(table_name, schema_entry):
    """Generate CREATE TABLE SQL for a single table."""
    target_table = f"dbo.com_5013_{table_name}"
    columns = schema_entry["columns"]
    
    # Sort columns by ordinal_position
    sorted_cols = sorted(columns, key=lambda x: x.get("ordinal_position", 999))
    
    sql_lines = [f"IF OBJECT_ID('{target_table}', 'U') IS NULL"]
    sql_lines.append("BEGIN")
    sql_lines.append(f"    CREATE TABLE {target_table} (")
    
    col_defs = []
    for col in sorted_cols:
        col_name = col["name"]
        sql_type = sql_type_from_schema(col, table_name=table_name)
        
        # Determine NULL/NOT NULL (assume nullable unless it's ID or primary key)
        nullable = "NULL"
        if col_name == "ID" and col["type"].upper() in ("BIGINT", "INT"):
            nullable = "NOT NULL"
        
        col_defs.append(f"        {col_name} {sql_type} {nullable}")
    
    sql_lines.append(",\n".join(col_defs))
    sql_lines.append("    );")
    sql_lines.append(f"    PRINT 'Table {target_table} created.';")
    sql_lines.append("END")
    sql_lines.append("ELSE")
    sql_lines.append("BEGIN")
    sql_lines.append(f"    PRINT 'Table {target_table} already exists.';")
    sql_lines.append("END;")
    sql_lines.append("GO")
    sql_lines.append("")
    
    return "\n".join(sql_lines)


def main():
    print(f"Reading schema from {SCHEMA_FILE}")
    print(f"Generating migration for {len(table_names)} tables...")
    
    output_lines = ["PRINT 'Creating replica tables from actual Xilnex schema';", "GO", ""]
    
    missing_tables = []
    for table_name in table_names:
        full_table_key = f"COM_5013.{table_name}"
        if full_table_key not in full_schema:
            # Try without schema prefix
            for key in full_schema.keys():
                if key.endswith(f".{table_name}"):
                    full_table_key = key
                    break
            else:
                missing_tables.append(table_name)
                print(f"[WARN] Warning: Table {table_name} not found in xilnex_full_schema.json")
                continue
        
        schema_entry = full_schema[full_table_key]
        table_sql = generate_table_sql(table_name, schema_entry)
        output_lines.append(table_sql)
        print(f"[OK] Generated SQL for {table_name} ({len(schema_entry['columns'])} columns)")
    
    if missing_tables:
        print(f"\n[WARN] Missing tables: {', '.join(missing_tables)}")
    
    output_content = "\n".join(output_lines)
    OUTPUT_FILE.write_text(output_content, encoding="utf-8")
    print(f"\n[OK] Migration file written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

