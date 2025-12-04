# VM Deployment Guide

Follow these steps to set up the ETL environment on your Windows Server VM.

## 1. Install Prerequisites

Download and install the following software on the VM. **Accept all default settings** unless noted.

1.  **Git for Windows**

    - Download: [git-scm.com/download/win](https://git-scm.com/download/win)
    - _Tip:_ Just click "Next" through all the options.

2.  **Python 3.13 (or 3.10+)**

    - Download: [python.org/downloads/windows](python)
    - **IMPORTANT:** On the first installer screen, check the box **"Add Python to PATH"** before clicking Install.

3.  **ODBC Driver 17 (or 18) for SQL Server**
    - Download: [Microsoft Download Page](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
    - _Note:_ Required for Python to talk to SQL Server. You likely already did this if you set up the DB.

---

## 2. Get the Code

Open **Command Prompt (cmd)** or **PowerShell** and run:

```powershell
cd C:\
git clone https://github.com/Wernjie0713/marrybrown_etl.git
cd marrybrown_etl
```

_(Replace the URL with your actual repo URL if different)_

---

## 3. Setup Python Environment

Run these commands inside the `marrybrown_etl` folder:

```powershell
# 1. Create a virtual environment (keeps dependencies clean)
python -m venv venv

# 2. Activate the environment
# You should see (venv) appear at the start of your command line
venv\Scripts\activate

# 3. Install dependencies (including Polars)
pip install -r requirements.txt
```

---

## 4. Configure Credentials

1.  Create a new file named `.env` in the `marrybrown_etl` folder.
2.  Open it with Notepad: `notepad .env`
3.  Paste your configuration. Use `localhost` for the target server since you are on the VM.

**Example `.env` content:**

```ini
# --- TARGET (LOCAL VM DB) ---
TARGET_DRIVER=ODBC Driver 17 for SQL Server
TARGET_SERVER=localhost
TARGET_DATABASE=MarryBrown_DW
TARGET_USERNAME=etl_user
TARGET_PASSWORD=YOUR_PASSWORD_HERE

# --- SOURCE (XILNEX CLOUD) ---
XILNEX_DRIVER=ODBC Driver 17 for SQL Server
XILNEX_SERVER=xilnex-mercury.database.windows.net
XILNEX_DATABASE=XilnexDB158
XILNEX_USERNAME=BI_5013_Marrybrown
XILNEX_PASSWORD=YOUR_XILNEX_PASSWORD_HERE

# --- SETTINGS ---
EXPORT_DIR=exports
```

4.  Save and close Notepad.

---

## 5. Run the Replication

You are now ready to run the orchestration script.

**Command:**

```powershell
# Make sure (venv) is active
python scripts/replicate_all_sales_data.py --start-date 2025-10-01 --end-date 2025-11-30 --max-workers 2
```

### Troubleshooting

- **"python is not recognized"**: You didn't check "Add to PATH" during installation. Reinstall Python or add it manually.
- **"IM002 Data source name not found"**: You missed installing the ODBC Driver.
- **"Module not found: polars"**: You forgot to run `pip install -r requirements.txt` or didn't activate the venv.
