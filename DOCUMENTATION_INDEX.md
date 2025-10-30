# Documentation Index - Marrybrown ETL Pipeline

**Quick navigation guide to all project documentation**

---

## 🚀 Getting Started (Start Here!)

| Document | Purpose | Read Time |
|----------|---------|-----------|
| **[docs/QUICKSTART.md](docs/QUICKSTART.md)** | Get the pipeline running in 10 minutes | 5 min |
| **[PROJECT_STATUS.md](PROJECT_STATUS.md)** | Current project status and progress | 3 min |
| **[README.md](README.md)** | Complete ETL pipeline documentation | 15 min |

---

## 📚 Core Documentation

### **Technical Reference**
| Document | Purpose | Audience |
|----------|---------|----------|
| **[docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)** | Complete star schema reference with all tables and relationships | Developers, Analysts |
| **[transform_sales_facts.sql](transform_sales_facts.sql)** | SQL transformation script for fact table | Developers |
| **[add_payment_type_key.sql](add_payment_type_key.sql)** | Schema update script for payment integration | DBAs |

### **Business Context**
| Document | Purpose | Audience |
|----------|---------|----------|
| **[docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md)** | Business problem, solution, and project phases | Management, Stakeholders |
| **[CHANGELOG.md](CHANGELOG.md)** | Version history and breaking changes | All |

### **Implementation Details**
| Document | Purpose | Audience |
|----------|---------|----------|
| **[docs/PAYMENT_TYPE_ETL_FIX.md](docs/PAYMENT_TYPE_ETL_FIX.md)** | Payment type integration technical details | Developers |
| **[DOCUMENTATION_UPDATES.md](DOCUMENTATION_UPDATES.md)** | Summary of recent documentation changes | All |

---

## 📖 Documentation by Role

### **For New Developers**
1. Start: [docs/QUICKSTART.md](docs/QUICKSTART.md)
2. Then: [README.md](README.md)
3. Reference: [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)
4. Deep Dive: [docs/PAYMENT_TYPE_ETL_FIX.md](docs/PAYMENT_TYPE_ETL_FIX.md)

### **For Business Analysts**
1. Start: [docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md)
2. Then: [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md) (focus on query examples)
3. Reference: [PROJECT_STATUS.md](PROJECT_STATUS.md)

### **For Management**
1. Start: [PROJECT_STATUS.md](PROJECT_STATUS.md)
2. Then: [docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md)
3. Reference: [CHANGELOG.md](CHANGELOG.md)

### **For Database Administrators**
1. Start: [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)
2. Then: [transform_sales_facts.sql](transform_sales_facts.sql)
3. Reference: [add_payment_type_key.sql](add_payment_type_key.sql)

---

## 🗂️ Documentation Structure

```
marrybrown_etl/
├── README.md                          # Main documentation
├── CHANGELOG.md                       # Version history
├── PROJECT_STATUS.md                  # Current status
├── DOCUMENTATION_INDEX.md             # This file
├── DOCUMENTATION_UPDATES.md           # Recent changes
├── transform_sales_facts.sql          # SQL transformation
├── add_payment_type_key.sql           # Schema update
├── etl_*.py                           # ETL scripts
├── requirements.txt                   # Python dependencies
│
└── docs/
    ├── QUICKSTART.md                  # 10-minute setup guide
    ├── DATABASE_SCHEMA.md             # Complete schema reference
    ├── PROJECT_CONTEXT.md             # Business context
    └── PAYMENT_TYPE_ETL_FIX.md        # Payment integration details
```

---

## 📝 Documentation Standards

### **Naming Convention**
- `README.md` - Main project documentation
- `UPPERCASE.md` - Project-level documents
- `docs/TitleCase.md` - Detailed technical documents

### **Update Frequency**
- **README.md**: Update when major features added
- **CHANGELOG.md**: Update with every version release
- **PROJECT_STATUS.md**: Update weekly
- **DATABASE_SCHEMA.md**: Update when schema changes
- **QUICKSTART.md**: Update when setup process changes

### **Maintenance**
All documentation is maintained by the project lead and should be reviewed:
- After each major feature implementation
- Before each project phase completion
- When onboarding new team members

---

## 🔄 Recent Updates (October 2025)

### **What's New**
- ✅ Added `CHANGELOG.md` for version tracking
- ✅ Added `PROJECT_STATUS.md` for current state overview
- ✅ Added `DOCUMENTATION_INDEX.md` (this file)
- ✅ Updated `README.md` with payment integration
- ✅ Updated `DATABASE_SCHEMA.md` with `PaymentTypeKey`
- ✅ Updated `QUICKSTART.md` with payment testing
- ✅ Updated `PROJECT_CONTEXT.md` with current phase status

### **What's Consistent**
- All documentation reflects payment type integration
- All code examples are up-to-date
- All schema references include `PaymentTypeKey`
- All test queries work with current database

---

## 🎯 Documentation Goals

### **Completeness** ✅
- All features are documented
- All scripts have usage examples
- All tables have schema definitions

### **Accuracy** ✅
- Documentation matches implementation
- Code examples are tested and working
- Schema diagrams reflect actual database

### **Accessibility** ✅
- Clear navigation structure
- Role-based reading paths
- Quick start for new users

### **Maintainability** ✅
- Consistent formatting
- Clear update procedures
- Version tracking

---

## 🆘 Documentation Help

### **Can't Find Something?**
1. Check this index first
2. Use Ctrl+F to search in documents
3. Check the README.md table of contents
4. Contact the project lead

### **Found an Error?**
1. Note the document name and section
2. Describe the issue clearly
3. Contact the project lead
4. Document will be updated promptly

### **Need More Detail?**
1. Check if a related technical document exists
2. Review the code comments in relevant scripts
3. Contact the project lead for clarification

---

## 📞 Documentation Maintainer

**YONG WERN JIE A22EC0121**  
MIS Department, Marrybrown Malaysia

For documentation questions, corrections, or suggestions, please reach out to the MIS team.

---

**Last Updated**: October 9, 2025  
**Documentation Version**: 1.1.0  
**Total Documents**: 11 files

