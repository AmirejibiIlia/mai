from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
import json
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql://", "postgresql+pg8000://", 1).replace("postgres://", "postgresql+pg8000://", 1)
_ENGINE_ARGS = {"connect_args": {"client_encoding": "utf8"}}


def ensure_metadata_table():
    """Create metadata table if it doesn't exist"""
    engine = create_engine(DATABASE_URL, **_ENGINE_ARGS)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS company_metadata (
                company_id VARCHAR(255) PRIMARY KEY,
                metadata JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.commit()
    print("✅ Metadata table ready\n")


def get_all_tables():
    """Get all tables except metadata table"""
    engine = create_engine(DATABASE_URL, **_ENGINE_ARGS)
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    return [t for t in tables if t != 'company_metadata']


def find_company_column(engine, tables):
    """Find which column identifies companies"""
    inspector = inspect(engine)
    possible = ['company_id', 'organization_id', 'tenant_id', 'client_id']
    
    for table in tables:
        cols = [col['name'] for col in inspector.get_columns(table)]
        for p in possible:
            if p in cols:
                return p
    return None


def get_companies(engine, tables, company_column):
    """Get all unique company IDs"""
    if not company_column:
        return ["default"]
    
    for table in tables:
        try:
            query = text(f"SELECT DISTINCT {company_column} FROM {table} WHERE {company_column} IS NOT NULL LIMIT 100")
            with engine.connect() as conn:
                result = conn.execute(query)
                companies = [str(row[0]) for row in result]
                if companies:
                    return companies
        except:
            continue
    return ["default"]


def get_unique_values(engine, table, column, company_filter):
    """Get sample unique values from a column"""
    try:
        if company_filter:
            query = text(f"SELECT DISTINCT {column} FROM {table} WHERE {company_filter} AND {column} IS NOT NULL LIMIT 50")
        else:
            query = text(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT 50")
        
        with engine.connect() as conn:
            return [str(r[0]) for r in conn.execute(query) if r[0]]
    except:
        return []


def build_and_save_metadata(engine, company_id, company_column, all_tables):
    """Build metadata for one company and save to database"""
    
    inspector = inspect(engine)
    
    if company_column:
        company_filter = f"{company_column} = '{company_id}'"
    else:
        company_filter = None
    
    metadata = {
        "company_id": company_id,
        "company_filter": company_filter or "1=1",
        "company_column": company_column,
        "tables": {}
    }
    
    print(f"  Scanning {len(all_tables)} tables...")
    
    for table in all_tables:
        print(f"    - {table}")
        
        cols = {}
        values = {}
        
        for col in inspector.get_columns(table):
            name = col["name"]
            col_type = str(col["type"]).lower()
            
            if name == company_column:
                continue
            
            cols[name] = col_type
            
            # Get sample values for text columns
            if any(t in col_type for t in ["char", "text", "varchar", "string"]):
                vals = get_unique_values(engine, table, name, company_filter)
                if vals and len(vals) < 100:
                    values[name] = vals
        
        metadata["tables"][table] = {
            "columns": cols,
            "values": values
        }
    
    # Save to database - FIXED: Use CAST instead of ::
    with engine.connect() as conn:
        # Check if exists
        result = conn.execute(
            text("SELECT 1 FROM company_metadata WHERE company_id = :id"),
            {"id": company_id}
        )
        exists = result.fetchone() is not None
        
        metadata_json = json.dumps(metadata)
        
        if exists:
            # Update existing
            conn.execute(text("""
                UPDATE company_metadata 
                SET metadata = CAST(:metadata AS jsonb), updated_at = NOW()
                WHERE company_id = :company_id
            """), {
                "company_id": company_id,
                "metadata": metadata_json
            })
        else:
            # Insert new
            conn.execute(text("""
                INSERT INTO company_metadata (company_id, metadata, updated_at)
                VALUES (:company_id, CAST(:metadata AS jsonb), NOW())
            """), {
                "company_id": company_id,
                "metadata": metadata_json
            })
        
        conn.commit()
    
    print(f"  ✅ Saved to database\n")


def main():
    """Main workflow"""
    
    print("🔍 Connecting to database...\n")
    engine = create_engine(DATABASE_URL, **_ENGINE_ARGS)
    
    # Ensure metadata table exists
    ensure_metadata_table()
    
    # Discover tables
    print("🔍 Discovering tables...")
    tables = get_all_tables()
    
    if not tables:
        print("❌ No tables found in database!")
        return
    
    print(f"✅ Found {len(tables)} tables: {tables}\n")
    
    # Find company column
    print("🔍 Looking for company column...")
    company_column = find_company_column(engine, tables)
    
    if company_column:
        print(f"✅ Found: '{company_column}'\n")
        companies = get_companies(engine, tables, company_column)
        print(f"✅ Found {len(companies)} companies\n")
    else:
        print("⚠️  No company column found (single-tenant mode)\n")
        companies = ["default"]
    
    # Build metadata for each company
    print("📝 Building metadata...\n")
    
    for company_id in companies:
        print(f"Company: {company_id}")
        build_and_save_metadata(engine, company_id, company_column, tables)
    
    print("✅ All metadata saved to 'company_metadata' table!\n")
    
    # Show what's in the database
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT company_id, updated_at, 
                   jsonb_object_keys(metadata->'tables') as table_names
            FROM company_metadata
        """))
        
        print("📋 Metadata in database:")
        current_company = None
        for row in result:
            if row[0] != current_company:
                if current_company:
                    print()
                print(f"  {row[0]} (updated: {row[1]})")
                current_company = row[0]
            print(f"    └─ {row[2]}")


if __name__ == "__main__":
    main()