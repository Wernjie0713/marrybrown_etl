"""
Generate Time Dimension Tables (dim_date and dim_time)
Populates date dimension for 2018-2025 and time dimension for full day

Author: YONG WERN JIE
Date: October 29, 2025
"""

import os
from datetime import date, time, timedelta
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load local environment variables (FakeRestaurantDB / local testing)
load_dotenv('.env.local')

def get_db_engine():
    """Creates a SQLAlchemy engine for the target warehouse."""
    driver = os.getenv("TARGET_DRIVER", "ODBC Driver 18 for SQL Server").replace(" ", "+")
    server = os.getenv("TARGET_SERVER")
    database = os.getenv("TARGET_DATABASE")
    user = os.getenv("TARGET_USERNAME")
    password = os.getenv("TARGET_PASSWORD")
    
    # URL-encode the password to handle special characters (@, !, etc.)
    encoded_password = quote_plus(password)
    
    connection_uri = (
        f"mssql+pyodbc://{user}:{encoded_password}@{server}/{database}?driver={driver}"
        "&TrustServerCertificate=yes&timeout=30"
    )
    
    return create_engine(connection_uri, pool_pre_ping=True, connect_args={"timeout": 30})


def generate_date_dimension(start_year=2018, end_year=2025):
    """
    Generate date dimension records from start_year to end_year
    
    Args:
        start_year: Starting year (default 2018)
        end_year: Ending year (default 2025)
    """
    print("="*80)
    print("GENERATING DATE DIMENSION (dim_date)")
    print("="*80)
    print(f"Date Range: {start_year}-01-01 to {end_year}-12-31")
    print()
    
    engine = get_db_engine()
    
    # Malaysian public holidays (add more as needed)
    holidays = [
        date(2018, 1, 1),   # New Year
        date(2018, 8, 31),  # Merdeka Day
        date(2018, 9, 16),  # Malaysia Day
        date(2018, 12, 25), # Christmas
        # Add more years...
        date(2019, 1, 1),
        date(2019, 8, 31),
        date(2019, 9, 16),
        date(2019, 12, 25),
    ]
    
    with engine.connect() as conn:
        # Clear existing data
        print("Clearing existing date dimension data...")
        conn.execute(text("DELETE FROM dbo.dim_date"))
        conn.commit()
        print("  [OK] Cleared")
        print()
        
        # Generate date records
        print("Generating date records...")
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)
        current_date = start_date
        count = 0
        
        while current_date <= end_date:
            date_key = int(current_date.strftime('%Y%m%d'))
            
            # Determine if weekend
            is_weekend = 1 if current_date.weekday() >= 5 else 0  # 5=Saturday, 6=Sunday
            
            # Determine if holiday
            is_holiday = 1 if current_date in holidays else 0
            
            # Day name
            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day_name = day_names[current_date.weekday()]
            
            # Month name
            month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            month_name = month_names[current_date.month - 1]
            
            # Quarter
            quarter = (current_date.month - 1) // 3 + 1
            
            # Insert record
            insert_query = text("""
                INSERT INTO dbo.dim_date (
                    DateKey, FullDate, DayOfWeek, DayName, DayOfMonth, DayOfYear,
                    WeekOfYear, MonthName, MonthOfYear, Quarter, Year, IsWeekend, IsHoliday
                ) VALUES (
                    :date_key, :full_date, :day_of_week, :day_name, :day_of_month, :day_of_year,
                    :week_of_year, :month_name, :month_of_year, :quarter, :year, :is_weekend, :is_holiday
                )
            """)
            
            conn.execute(insert_query, {
                'date_key': date_key,
                'full_date': current_date,
                'day_of_week': current_date.weekday() + 1,  # 1=Monday, 7=Sunday
                'day_name': day_name,
                'day_of_month': current_date.day,
                'day_of_year': current_date.timetuple().tm_yday,
                'week_of_year': current_date.isocalendar()[1],
                'month_name': month_name,
                'month_of_year': current_date.month,
                'quarter': quarter,
                'year': current_date.year,
                'is_weekend': is_weekend,
                'is_holiday': is_holiday
            })
            
            count += 1
            current_date += timedelta(days=1)
        
        conn.commit()
        print(f"  [OK] Generated {count:,} date records")
        print()


def generate_time_dimension():
    """
    Generate time dimension records for every minute of the day (00:00:00 to 23:59:00)
    """
    print("="*80)
    print("GENERATING TIME DIMENSION (dim_time)")
    print("="*80)
    print("Time Range: 00:00:00 to 23:59:00 (1,440 records)")
    print()
    
    engine = get_db_engine()
    
    with engine.connect() as conn:
        # Clear existing data
        print("Clearing existing time dimension data...")
        conn.execute(text("DELETE FROM dbo.dim_time"))
        conn.commit()
        print("  [OK] Cleared")
        print()
        
        # Generate time records (every minute)
        print("Generating time records...")
        count = 0
        
        for hour in range(24):
            for minute in range(60):
                current_time = time(hour, minute, 0)
                
                # TimeKey format: HHMMSS as integer (e.g., 143000 for 14:30:00)
                time_key = int(current_time.strftime('%H%M%S'))
                
                # Determine period of day
                if 0 <= hour < 6:
                    period = "Early Morning"
                elif 6 <= hour < 12:
                    period = "Morning"
                elif 12 <= hour < 17:
                    period = "Afternoon"
                elif 17 <= hour < 21:
                    period = "Evening"
                else:
                    period = "Night"
                
                # Hour name (12-hour format)
                if hour == 0:
                    hour_name = "12 AM"
                elif hour < 12:
                    hour_name = f"{hour} AM"
                elif hour == 12:
                    hour_name = "12 PM"
                else:
                    hour_name = f"{hour-12} PM"
                
                # Insert record
                insert_query = text("""
                    INSERT INTO dbo.dim_time (
                        TimeKey, FullTime, Hour, Minute, Second, HourName, PeriodOfDay
                    ) VALUES (
                        :time_key, :full_time, :hour, :minute, :second, :hour_name, :period_of_day
                    )
                """)
                
                conn.execute(insert_query, {
                    'time_key': time_key,
                    'full_time': current_time,
                    'hour': hour,
                    'minute': minute,
                    'second': 0,
                    'hour_name': hour_name,
                    'period_of_day': period
                })
                
                count += 1
        
        conn.commit()
        print(f"  [OK] Generated {count:,} time records")
        print()


def verify_dimensions():
    """Verify the generated dimensions"""
    print("="*80)
    print("VERIFYING TIME DIMENSIONS")
    print("="*80)
    print()
    
    engine = get_db_engine()
    
    with engine.connect() as conn:
        # Check dim_date
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as TotalDates,
                MIN(FullDate) as MinDate,
                MAX(FullDate) as MaxDate,
                SUM(CAST(IsWeekend AS INT)) as WeekendDays,
                SUM(CAST(IsHoliday AS INT)) as Holidays
            FROM dbo.dim_date
        """))
        row = result.fetchone()
        
        print("dim_date:")
        print(f"  Total Records: {row[0]:,}")
        print(f"  Date Range: {row[1]} to {row[2]}")
        print(f"  Weekend Days: {row[3]:,}")
        print(f"  Holidays: {row[4]:,}")
        print()
        
        # Check dim_time
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as TotalTimes,
                MIN(FullTime) as MinTime,
                MAX(FullTime) as MaxTime
            FROM dbo.dim_time
        """))
        row = result.fetchone()
        
        print("dim_time:")
        print(f"  Total Records: {row[0]:,}")
        print(f"  Time Range: {row[1]} to {row[2]}")
        print()


def main():
    """Main execution function"""
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║          TIME DIMENSION GENERATION FOR CLOUD WAREHOUSE         ║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()
    
    try:
        # Generate date dimension (2018-2025)
        generate_date_dimension(start_year=2018, end_year=2025)
        
        # Generate time dimension (full day)
        generate_time_dimension()
        
        # Verify results
        verify_dimensions()
        
        print()
        print("╔════════════════════════════════════════════════════════════════╗")
        print("║              TIME DIMENSIONS GENERATED SUCCESSFULLY!           ║")
        print("╚════════════════════════════════════════════════════════════════╝")
        print()
        
    except Exception as e:
        print(f"\n[ERROR] Failed to generate time dimensions: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

