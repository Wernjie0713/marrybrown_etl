# 🔥 Firewall Issue: dim_terminals ETL

## ❌ **Problem:**

```
Cannot open server 'xilnex-mercury' requested by the login. 
Client with IP address '61.6.141.113' is not allowed to access the server.
```

**Translation:** Your **TIMEdotcom cloud server** (IP: `61.6.141.113`) is trying to connect to the Xilnex Azure SQL database, but it's **blocked by the firewall**.

---

## 🔍 **Why This Happens:**

- Your **local PC** is whitelisted in Xilnex Azure firewall → ✅ Works locally
- Your **cloud server** is NOT whitelisted → ❌ Blocked

---

## ✅ **SOLUTIONS:**

### **Option 1: Whitelist Cloud Server IP (RECOMMENDED)**

Contact Xilnex support to add this IP to the Azure SQL firewall:
```
IP Address: 61.6.141.113
```

**Pros:** Simple, all ETL scripts work as-is  
**Cons:** Requires vendor coordination

---

### **Option 2: Run ETL from Local PC, Push to Cloud (TEMPORARY)**

Run dimension ETL scripts on your **local PC**, then copy the data to cloud:

```powershell
# On LOCAL PC (has Xilnex access)
python etl_dim_terminals.py  # Uses .env file pointing to cloud warehouse

# Your local PC can:
# - Extract from Xilnex Azure (whitelisted IP)
# - Load to Cloud warehouse (via VPN)
```

**Pros:** Works immediately, no vendor coordination  
**Cons:** Requires VPN connection, manual orchestration

---

### **Option 3: Use Xilnex API Instead (FUTURE)**

Since the API doesn't have firewall restrictions, use it for terminal data:

**Pros:** No firewall issues, cloud-native  
**Cons:** Requires API endpoint for terminals (may not exist)

---

## 🎯 **RECOMMENDED ACTION:**

**Short-term:** Use **Option 2** - Run `etl_dim_terminals.py` from your **local PC** while connected to **VPN**, but configure `.env.cloud` to load to the cloud warehouse.

**Long-term:** Request Xilnex to whitelist `61.6.141.113` in their Azure SQL firewall.

---

## 📝 **How to Run from Local PC:**

```powershell
# 1. Connect to VPN
# 2. Ensure .env.cloud has correct cloud warehouse credentials
# 3. Run on local PC (which can access both Xilnex and cloud):

python etl_dim_terminals.py
```

The script will:
- ✅ Extract from Xilnex (local PC IP is whitelisted)
- ✅ Load to Cloud warehouse (via VPN connection)

