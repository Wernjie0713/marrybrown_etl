# Project Context: Xilnex Data Liberation Initiative

**Company**: Marrybrown Malaysia  
**Department**: Management Information Systems (MIS)  
**Project Lead**: YONG WERN JIE A22EC0121 (Intern)  
**Status**: Phase 4 - API Development (In Progress) | Phase 5 - Portal Development (In Progress)

---

## 🎯 Executive Summary

Marrybrown's operational data is currently locked within the third-party Xilnex POS system, with limited, costly access that severely restricts business analytics capabilities. This project creates an independent, company-owned data ecosystem to provide full control over business data, enabling flexible reporting, advanced analytics, and future innovation.

---

## 🚀 The Mission

> **"Liberate Marrybrown's data from the restrictive Xilnex system and create a modern, scalable data infrastructure for business intelligence."**

---

## ❓ The Problem

### Before This Project

1. **Data Silos**: All operational data trapped in Xilnex POS system
2. **Limited Access**: Restrictive API with rate limits and high costs
3. **Poor Performance**: OLTP database unable to handle analytical queries (consistent timeouts)
4. **Vendor Lock-in**: Dependent on third-party for all data needs
5. **No Custom Analytics**: Unable to build custom reports or dashboards
6. **Slow Insights**: Manual data extraction for any analysis

### Business Impact

- ❌ Cannot perform ad-hoc sales analysis
- ❌ Limited customer behavior insights
- ❌ No real-time operational dashboards
- ❌ Expensive and slow custom reporting
- ❌ Inability to leverage modern BI tools

---

## ✅ The Solution

### The Two-System Architecture

```
┌─────────────────────────────────────────────────────────┐
│  SYSTEM 1: Xilnex POS (OLTP)                           │
│  ─────────────────────────────────────                  │
│  • Handles daily transactions                           │
│  • Optimized for fast writes                            │
│  • Managed by Xilnex (read-only access)                 │
│  • NOT MODIFIED - continues normal operations           │
└────────────────────┬────────────────────────────────────┘
                     │
                     │ ETL/ELT Pipeline
                     │ (Python + SQL)
                     ▼
┌─────────────────────────────────────────────────────────┐
│  SYSTEM 2: Cloud Data Warehouse (OLAP)                 │
│  ───────────────────────────────────────                │
│  • Optimized for analytics and reporting                │
│  • Star schema design                                   │
│  • Fully controlled by Marrybrown                       │
│  • Continuously updated from System 1                   │
└────────────────────┬────────────────────────────────────┘
                     │
                     │ REST API
                     │ (FastAPI)
                     ▼
┌─────────────────────────────────────────────────────────┐
│  Business Intelligence Layer                            │
│  ─────────────────────────────────                      │
│  • Web-based reporting portal                           │
│  • Custom dashboards                                    │
│  • Real-time KPI monitoring                             │
│  • Ad-hoc query capabilities                            │
└─────────────────────────────────────────────────────────┘
```

### Key Deliverables

1. ✅ **Cloud Data Warehouse** (COMPLETE)
   - Star schema with fact and dimension tables
   - Hosted on TIMEdotcom cloud infrastructure
   - Optimized for fast analytical queries

2. ✅ **ETL/ELT Pipeline** (COMPLETE)
   - Python scripts for dimension tables
   - Multithreaded data extraction
   - SQL-based transformation for large datasets
   - Payment method tracking integrated
   - Continuously replicates from Xilnex

3. 🔄 **REST API** (IN PROGRESS)
   - FastAPI backend with JWT authentication
   - Sales KPI endpoints operational
   - Daily sales summary endpoint
   - Product mix report endpoint
   - EOD sales summary with payment breakdown
   - Location management endpoints

4. 🔄 **Reporting Portal** (IN PROGRESS)
   - React-based web application
   - Interactive dashboards
   - User authentication and role-based access

---

## 🏗️ Technical Architecture

### Data Flow

```
Xilnex POS DB
    │
    │ Extract (Python + SQLAlchemy)
    ▼
Python ETL Scripts
    │ Transform (Pandas)
    ▼
Staging Tables ─────┐
    │               │
    │ Transform     │ Load
    │ (SQL)         │ (Direct)
    ▼               ▼
Fact Tables    Dimension Tables
    │               │
    └───────┬───────┘
            │
    Star Schema (OLAP)
            │
            │ Query (SQL)
            ▼
    FastAPI Backend
            │
            │ REST/JSON
            ▼
    React Frontend
```

### Technology Stack

#### Data Layer
- **Source**: Microsoft SQL Server (Xilnex)
- **Target**: Microsoft SQL Server (TIMEdotcom Cloud)
- **Schema**: Star Schema (dimensional model)

#### ETL Layer
- **Language**: Python 3.13
- **Libraries**: Pandas, SQLAlchemy, PyODBC
- **Pattern**: Hybrid ETL/ELT

#### API Layer
- **Framework**: FastAPI
- **Server**: Uvicorn (ASGI)
- **Validation**: Pydantic v2
- **Authentication**: JWT (planned)

#### Frontend Layer (Planned)
- **Framework**: React
- **Charts**: Recharts / Chart.js
- **UI**: TailwindCSS / Material-UI

---

## 📊 The Data Model

### Star Schema Design

**Central Fact Table:**
- `fact_sales_transactions` - Every line item on every receipt

**Surrounding Dimensions:**
- `dim_date` - Calendar (4,018 days)
- `dim_time` - Time of day (86,400 seconds)
- `dim_products` - Menu items (~2,000)
- `dim_customers` - Loyalty members (~100,000)
- `dim_locations` - Store outlets (~200)
- `dim_staff` - Cashiers and systems (~1,000)
- `dim_promotions` - Vouchers and deals (~500)
- `dim_payment_types` - Payment methods (~10)

### Why Star Schema?

1. **Fast Queries**: Minimal joins required
2. **Intuitive**: Easy to understand for business users
3. **Flexible**: New dimensions can be added easily
4. **Optimized**: Database engines optimize for this pattern

---

## 🔍 Key Discoveries from Analysis

### Critical Findings

1. **Application-Layer Recipe Logic** 🔴
   - Combo meal recipes NOT stored in database
   - Logic embedded in Xilnex application
   - Must be manually reverse-engineered
   - **Risk**: Requires operational staff interviews

2. **Severe Performance Bottlenecks** ⚠️
   - Queries on high-volume tables timeout consistently
   - Varchar dates instead of proper DATETIME types
   - Insufficient covering indexes
   - **Proof**: Xilnex cannot support modern analytics

3. **Strong Data Quality** ✅
   - Excellent referential integrity
   - No orphan records in recent data
   - Complete core transactional data
   - **Benefit**: Clean data for migration

4. **Business Logic Discovered** 💡
   - Returns: `SALES_TYPE = 'Return'` with negative amounts
   - Loyalty points: Tracked in `double_accumulate_value`
   - Multiple product code systems requiring mapping

---

## 📈 Project Phases

### Phase 1: Database Analysis ✅ COMPLETE

**Duration**: 3 weeks  
**Deliverables**:
- Comprehensive reconnaissance report
- Data profiling analysis
- Performance analysis
- 400+ pages of documentation in Notion

**Key Activities**:
- Mapped all key tables and relationships
- Profiled data quality and integrity
- Identified performance bottlenecks
- Discovered critical business logic

---

### Phase 2: Database Design & Deployment ✅ COMPLETE

**Duration**: 2 weeks  
**Deliverables**:
- Star schema design
- CREATE TABLE statements
- Index strategy
- Deployed cloud database

**Key Activities**:
- Designed dimensional model
- Created all tables with proper data types
- Implemented surrogate key strategy
- Liaised with TIMEdotcom for provisioning

---

### Phase 3: ETL Pipeline Development ✅ COMPLETE

**Duration**: 3 weeks  
**Deliverables**:
- 7 dimension ETL scripts
- 1 fact ELT script with multithreading
- SQL transformation script
- Time dimension generator

**Key Activities**:
- Built Python ETL scripts for all dimensions
- Implemented multithreaded fact data extraction
- Created SQL transformation logic
- Tested with 7-day rolling windows

**Current Status**:
- ✅ All dimension tables populated
- ✅ Fact table extraction working
- ✅ Transform SQL script operational
- ⏳ Waiting for bulk CSV export from Xilnex admin

---

### Phase 4: API Development 🔄 IN PROGRESS

**Duration**: 3 weeks (estimated)  
**Target Deliverables**:
- REST API with comprehensive endpoints
- Authentication and authorization
- API documentation
- Performance optimization

**Completed**:
- ✅ FastAPI project structure
- ✅ Database connection layer
- ✅ First endpoint: `/sales/kpi/summary`

**Remaining**:
- 📝 Additional sales endpoints (date ranges, location filters)
- 📝 Product analytics endpoints
- 📝 Customer analytics endpoints
- 📝 Location performance endpoints
- 📝 JWT authentication
- 📝 Role-based access control

---

### Phase 5: Frontend Development 📝 PLANNED

**Duration**: 4 weeks (estimated)  
**Deliverables**:
- React web application
- Interactive dashboards
- User management
- Production deployment

---

## 🎓 Learning Objectives (Internship)

### Technical Skills Developed

1. **Data Engineering**
   - ETL/ELT design patterns
   - Star schema modeling
   - Data warehouse architecture
   - Performance optimization

2. **Database Management**
   - SQL Server administration
   - Query optimization
   - Index strategy
   - Data profiling

3. **Backend Development**
   - FastAPI framework
   - REST API design
   - SQLAlchemy ORM
   - Async programming

4. **Python Programming**
   - Pandas data manipulation
   - Multithreading
   - Error handling
   - Code organization

5. **DevOps Basics**
   - Environment management
   - Connection pooling
   - Configuration management
   - Documentation practices

---

## 📚 Documentation Structure

### This Repository (ETL)

```
marrybrown_etl/
├── README.md              ← Start here
├── PROJECT_CONTEXT.md     ← This file (big picture)
├── DATABASE_SCHEMA.md     ← Star schema reference
├── etl_*.py               ← Individual ETL scripts
├── transform_sales_facts.sql  ← SQL transformation
└── data/                  ← Data files (if any)
```

### API Repository

```
marrybrown_api/
├── README.md              ← API overview
├── API_ENDPOINTS.md       ← Endpoint reference
├── main.py                ← FastAPI app
├── database.py            ← DB connections
└── routers/               ← API routes
    └── sales.py           ← Sales endpoints
```

### Notion Documentation

- 📄 Project Plan & Roadmap
- 📄 Xilnex Database Reconnaissance Report
- 📄 Xilnex Database Analysis Report
- 🏛️ Project Architecture Documentation
- 🗺️ ETL Source-to-Target Mapping
- 🗃️ Complete Database Schema with ERDs
- 📊 Individual table specifications

---

## 🔑 Key Decisions & Rationale

### Why Star Schema?
- **Decision**: Use dimensional modeling instead of normalized 3NF
- **Rationale**: Optimized for analytical queries, intuitive for business users, industry standard for data warehouses

### Why Hybrid ETL/ELT?
- **Decision**: Use ETL for dimensions, ELT for facts
- **Rationale**: Small tables transform efficiently in Python; large tables benefit from database engine's parallel processing

### Why FastAPI?
- **Decision**: Use FastAPI instead of Django/Flask
- **Rationale**: Modern async support, automatic documentation, type safety, excellent performance

### Why SQL Transformation?
- **Decision**: Use SQL instead of Spark for fact table transformation
- **Rationale**: Simpler, cost-effective, sufficient for current scale, leverages database optimizations

### Why Python Extraction (Current)?
- **Decision**: Use Python to extract data instead of bulk export
- **Rationale**: Temporary solution for learning while waiting for Xilnex admin; demonstrates full-stack capability; production will use bulk export

---

## 🎯 Success Metrics

### Technical Success

- [x] All tables created with proper data types
- [x] ETL pipeline successfully loads dimensions
- [x] Fact data extraction functional
- [x] Transform SQL performs joins correctly
- [ ] API responds in < 1 second for KPI queries
- [ ] Frontend displays data in real-time

### Business Success

- [ ] Management can access daily KPIs without vendor
- [ ] Ad-hoc queries execute in seconds (vs. timeout before)
- [ ] Custom reports available without additional cost
- [ ] Data freshness: < 24 hours old

---

## ⚠️ Risks & Mitigations

### Risk 1: Recipe Logic Not in Database
**Impact**: HIGH  
**Mitigation**: Interview operations staff to document manually; validate against Xilnex reports

### Risk 2: Xilnex Admin Delays
**Impact**: MEDIUM  
**Mitigation**: Continue with Python extraction approach; demonstrates value without blocking

### Risk 3: Performance Issues at Scale
**Impact**: MEDIUM  
**Mitigation**: Implement proper indexing; use aggregation tables if needed; optimize queries

### Risk 4: Intern Departure
**Impact**: HIGH  
**Mitigation**: Comprehensive documentation (like this!); knowledge transfer sessions; clean, readable code

---

## 🤝 Team & Stakeholders

### Development Team
- **Intern Developer**: YONG WERN JIE A22EC0121
- **MIS Supervisor**: [Name]
- **Database Admin**: TIMEdotcom team

### Stakeholders
- **Primary**: MIS Department (data consumers)
- **Secondary**: Management (report users)
- **Tertiary**: Operations (data source)

---

## 📅 Timeline

| Phase | Duration | Status |
|-------|----------|--------|
| Phase 1: Analysis | 3 weeks | ✅ Complete |
| Phase 2: Database Design | 2 weeks | ✅ Complete |
| Phase 3: ETL Development | 3 weeks | ✅ Complete |
| Phase 4: API Development | 3 weeks | 🔄 In Progress |
| Phase 5: Frontend | 4 weeks | 📝 Planned |
| **Total** | **15 weeks** | **Week 8** |

---

## 🔮 Future Enhancements

### Near-Term (3-6 months)
- Real-time Change Data Capture (CDC)
- Apache Airflow orchestration
- Automated data quality checks
- Email alerts and monitoring

### Long-Term (6-12 months)
- Machine learning models for demand forecasting
- Customer segmentation and personalization
- Real-time inventory optimization
- Integration with accounting systems

---

## 📖 Learning Resources

### Documentation
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Star Schema Design](https://www.kimballgroup.com/)

### Related Concepts
- **OLTP vs OLAP**: Transaction vs Analytical databases
- **ETL vs ELT**: Transform location strategies
- **Star Schema**: Dimensional modeling pattern
- **Surrogate Keys**: Artificial primary keys for data warehouses

---

## 💡 Key Insights for New Team Members

1. **This is a liberation project**: We're not replacing Xilnex, we're creating a parallel analytics system

2. **Two systems, two purposes**: Xilnex for transactions, our warehouse for analytics

3. **Data quality is excellent**: Recent transactional data is clean and reliable

4. **Performance was the main driver**: Xilnex database cannot handle analytical queries

5. **Documentation is critical**: Everything is documented for knowledge transfer

6. **Pragmatic approach**: Using temporary Python extraction while waiting for production bulk export

7. **Learning-focused**: Built to learn full data engineering stack, not just complete task

---

## 📞 Getting Help

### Quick Start Guides
- **ETL**: See `README.md` in this directory
- **API**: See `README.md` in `marrybrown_api` directory
- **Schema**: See `DATABASE_SCHEMA.md`
- **Endpoints**: See `API_ENDPOINTS.md` in API directory

### For Questions
- Check Notion documentation for detailed analysis
- Review code comments in Python scripts
- Consult database schema diagrams
- Contact MIS team

---

## 🎓 What Makes This Project Special

1. **Real Business Impact**: Solves actual company pain points
2. **Full Stack**: Covers database, backend, and (planned) frontend
3. **Modern Patterns**: Uses industry-standard data engineering practices
4. **Well-Documented**: Extensive documentation for maintainability
5. **Production-Ready**: Built with scalability and performance in mind
6. **Learning-Oriented**: Demonstrates complete data engineering workflow

---

**Last Updated**: October 2025  
**Maintained By**: YONG WERN JIE A22EC0121  
**For**: Marrybrown Malaysia MIS Department

