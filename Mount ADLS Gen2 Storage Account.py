# Databricks notebook source
# DBTITLE 1,Direct ADLS Gen2 Access using Access Key
# Direct ADLS Gen2 Access using Storage Account Access Key
# No mount needed — works on all cluster types including Shared (USER_ISOLATION)

# ---- Configuration ----
storage_account_name = "staffbaseadlspoc"
container_name = "raw"

# Using Databricks Secret Scope (secure — no hardcoded keys)
access_key = dbutils.secrets.get(scope="adls-poc", key="storage-account-key")

# ---- Set Access Key in Spark Config ----
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    access_key
)

print(f"Access configured for storage account: {storage_account_name}")
print(f"You can now read/write using: abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/")

# COMMAND ----------

# DBTITLE 1,Verify Access and List Files
# Verify access by listing files in the ADLS container
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

display(dbutils.fs.ls(adls_path))

# COMMAND ----------

# DBTITLE 1,Step 1 — Read Batch Config from ADLS
# ============================================================
# Step 1: Read Batch Configuration from ADLS
# Searches common locations; falls back to defaults if not found
# ============================================================
import json

# ---- Storage variables (mirrors Cell 1; redefined here for standalone execution) ----
storage_account_name = "staffbaseadlspoc"
container_name       = "raw"
adls_base_path       = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"

# ---- Search ADLS for a config file ----
print("Searching for batch config file in ADLS...")
config           = None
config_file_path = None

search_paths = [
    adls_base_path,
    f"{adls_base_path}/config",
    f"{adls_base_path}/staffbase_input",
    f"{adls_base_path}/poc_output",
]

for sp in search_paths:
    try:
        for f in dbutils.fs.ls(sp):
            fname = f.name.lower()
            if ("config" in fname or fname.endswith(".json") or fname.endswith(".yaml") or fname.endswith(".txt")) \
               and not f.name.startswith("part-"):
                print(f"  Candidate: {f.path}")
                config_file_path = f.path   # keep last match; update if you want first
    except Exception:
        pass

if config_file_path:
    try:
        raw     = spark.read.text(config_file_path).collect()
        content = "\n".join(r[0] for r in raw)
        config  = json.loads(content)
        print(f"\nConfig loaded from: {config_file_path}")
        print(json.dumps(config, indent=2))
    except Exception as e:
        print(f"Warning: could not parse config ({e}) — using defaults.")

if config is None:
    print("\nNo batch config found — using default values.")
    config = {}

# ---- Resolve values (supports both snake_case and camelCase keys) ----
BATCH_SIZE    = int(config.get("batch_size",    config.get("batchSize",    50_000)))
OUTPUT_FOLDER = config.get("output_folder",    config.get("outputFolder", f"{adls_base_path}/staffbase_input/"))
FILE_PREFIX   = config.get("file_prefix",      config.get("filePrefix",   "employee_batch"))
TOTAL_RECORDS = int(config.get("total_records", config.get("totalRecords", 400_000)))
NUM_BATCHES   = -(-TOTAL_RECORDS // BATCH_SIZE)   # ceiling division

print(f"\n{'='*55}")
print("Effective Batch Configuration")
print(f"{'='*55}")
print(f"  Total Records  : {TOTAL_RECORDS:,}")
print(f"  Batch Size     : {BATCH_SIZE:,}")
print(f"  Num Batches    : {NUM_BATCHES}")
print(f"  Output Folder  : {OUTPUT_FOLDER}")
print(f"  File Prefix    : {FILE_PREFIX}")

# COMMAND ----------

# DBTITLE 1,Step 2 — Generate 400K Employee Records (Core HCM Schema)
# ============================================================
# Step 2: Generate 400K Dummy Employee Records
# Schema: Core HCM employee_general (27 columns)
# ============================================================
from pyspark.sql.functions import (
    array, element_at, col, lit, concat, lpad, lower,
    when, date_add, floor
)

# ---- Lookup tables (used as Spark array literals) ----
FIRST_NAMES = [
    "James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
    "William","Barbara","David","Elizabeth","Richard","Susan","Joseph","Jessica",
    "Thomas","Sarah","Charles","Karen","Christopher","Lisa","Daniel","Nancy",
    "Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley",
    "Steven","Dorothy","Paul","Kimberly","Andrew","Emily","Joshua","Donna",
    "Kenneth","Michelle","Kevin","Carol","Brian","Amanda","George","Melissa",
    "Timothy","Deborah"
]  # 50

LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
    "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson",
    "White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker",
    "Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
    "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell",
    "Carter","Roberts"
]  # 50

EMP_STATUSES   = ["Active"]*7 + ["Terminated"]*2 + ["Leave_of_Absence", "Suspended"]   # 11
DISTRICTS      = [f"D{str(i).zfill(3)}" for i in range(1, 26)]                          # 25
DEPARTMENTS    = [
    "HR","Finance","IT","Operations","Marketing","Sales","Legal","Compliance",
    "Supply_Chain","Customer_Service","Real_Estate","Pharmacy","Fuel_Center",
    "Bakery","Deli","Produce","Meat","Grocery","Digital","Analytics"
]  # 20
JOB_CODES      = [f"JOB{str(i).zfill(4)}" for i in range(1, 51)]                        # 50
JOB_TITLES     = [
    "Store Manager","Assistant Manager","Department Head","Team Lead","Associate",
    "Senior Associate","Cashier","Stocker","Pharmacist","Pharmacy Tech",
    "IT Analyst","Data Engineer","HR Business Partner","Finance Analyst",
    "Marketing Manager","Sales Representative","Operations Coordinator",
    "Compliance Officer","Legal Counsel","VP Operations","District Manager",
    "Division VP","Customer Service Rep","Produce Manager","Deli Manager",
    "Bakery Manager","Meat Manager","Fuel Center Attendant","Digital Analyst",
    "Supply Chain Analyst"
]  # 30
JOB_FAMILIES   = [
    "Management","Technical","Operations","Administrative","Customer_Service",
    "Finance","Legal","HR","Marketing","Digital"
]  # 10
DIVISION_CODES = [f"DIV{str(i).zfill(2)}" for i in range(1, 11)]                       # 10
UNION_CODES    = ["UFCW123","UFCW456","LOCAL789","TEAMSTERS101","","","","","",""]   # 10  (40% unionized)
LOC_CODES      = [f"LOC{str(i).zfill(5)}" for i in range(1, 101)]                      # 100
BIZ_DOMAINS    = ["Corporate","Retail","Digital","Operations","Healthcare"]              # 5
MASTER_DEPTS   = [
    "Store_Operations","Corporate_Functions","Digital_Tech",
    "Supply_Chain_Ops","Healthcare_Wellness"
]  # 5

def la(lst):
    """Build a Spark array literal from a Python list."""
    return array(*[lit(v) for v in lst])

# ============================================================
# POC OVERRIDE — set to 101 records for initial Staffbase test
# To switch back to full 400K load, comment line A and
# uncomment line B
# ============================================================
TOTAL_RECORDS = 101   # (A) POC test size  ← active
# TOTAL_RECORDS = TOTAL_RECORDS  # (B) full load from Step 1 config  ← commented
NUM_BATCHES   = 1     # single batch is enough for 101 records

# ---- Step A: compute all intermediate lookup columns in one pass ----
base_df = (
    spark.range(TOTAL_RECORDS)
    .select(
        col("id"),
        element_at(la(FIRST_NAMES),    ((col("id") % 50)  + 1).cast("int")).alias("_fn"),
        element_at(la(LAST_NAMES),     ((floor(col("id") / 50) % 50 + 1).cast("int"))).alias("_ln"),
        element_at(la(EMP_STATUSES),   ((col("id") % 11)  + 1).cast("int")).alias("_status"),
        element_at(la(DISTRICTS),      ((col("id") % 25)  + 1).cast("int")).alias("_district"),
        element_at(la(DEPARTMENTS),    ((col("id") % 20)  + 1).cast("int")).alias("_dept"),
        element_at(la(JOB_CODES),      ((col("id") % 50)  + 1).cast("int")).alias("_jcode"),
        element_at(la(JOB_TITLES),     ((col("id") % 30)  + 1).cast("int")).alias("_jtitle"),
        element_at(la(JOB_FAMILIES),   ((col("id") % 10)  + 1).cast("int")).alias("_jfam"),
        element_at(la(DIVISION_CODES), ((col("id") % 10)  + 1).cast("int")).alias("_div"),
        element_at(la(UNION_CODES),    ((col("id") % 10)  + 1).cast("int")).alias("_ucode"),
        element_at(la(LOC_CODES),      ((col("id") % 100) + 1).cast("int")).alias("_loc"),
        element_at(la(BIZ_DOMAINS),    ((col("id") % 5)   + 1).cast("int")).alias("_bdom"),
        element_at(la(MASTER_DEPTS),   ((col("id") % 5)   + 1).cast("int")).alias("_mdept"),
    )
)

# ---- Step B: derive the 27 final employee columns ----
# Column headers exactly match Staffbase exported CSV column names
# so the import engine maps them automatically without manual remapping.
# Source reference: exported from https://krogertest.staffbase.com/studio/users
df_employees = base_df.select(

    # ── Core Staffbase identity / profile fields ──────────────────────────
    concat(lit("KXI"), lpad((col("id") + 1_000_000).cast("string"), 7, "0")).alias("kxid"),

    # Identifier = Staffbase primary external ID shown in Account Info (used for SSO/IAM linking)
    # In production this will be the PingOne / IAM ID.
    # For POC dummy data we use email as the identifier (matches existing users pattern)
    lower(concat(
        col("_fn"), lit("."), col("_ln"),
        (col("id") % 1000).cast("string"),
        lit("@kroger.com")
    )).alias("Identifier"),

    col("_fn").alias("First Name"),
    col("_ln").alias("Last Name"),
    when(col("id") % 8 == 0, concat(col("_fn"), lit("_Nick")))
        .otherwise(col("_fn")).alias("Preferred Name"),
    lower(concat(col("_fn"), lit("."), col("_ln"),
        (col("id") % 1000).cast("string"), lit("@kroger.com"))).alias("Email Address"),
    col("_loc").alias("HR Location Code"),
    concat(lit("Kroger Store #"), lpad(((col("id") % 100) + 1).cast("string"), 5, "0")).alias("Location"),
    col("_dept").alias("Department Code"),
    concat(col("_dept"), lit(" Department")).alias("Department Description"),
    col("_mdept").alias("Master Department"),
    col("_jtitle").alias("Job Title"),
    col("_jcode").alias("Job Code"),
    col("_jfam").alias("Job Family"),
    col("_div").alias("Division"),
    col("_bdom").alias("Business Domain"),
    col("_district").alias("District"),

    # ── Employment ────────────────────────────────────────────────────────
    # Staffbase accepts "activated" / "deactivated"
    when(col("_status") == lit("Active"), lit("activated"))
        .otherwise(lit("deactivated")).alias("Employment Status"),
    date_add(lit("1990-01-01").cast("date"), (col("id") % (365 * 34)).cast("int")).alias("Hire Date"),
    when(col("id") % 100 == 0, lit("true")).otherwise(lit("false")).alias("Executive"),

    # ── Person identifiers ────────────────────────────────────────────────
    (col("id") + 100_000).cast("string").alias("Person Number"),
    (floor(col("id") / 10) + 100_000).cast("string").alias("Manager Person Number"),

    # ── Union ─────────────────────────────────────────────────────────────
    col("_ucode").alias("Union"),
    when(col("_ucode") != lit(""),
        when(col("id") % 4 == 0, lit("UFCW Local 123"))
        .when(col("id") % 4 == 1, lit("Teamsters Local 456"))
        .when(col("id") % 4 == 2, lit("UFCW Local 789"))
        .otherwise(lit("SEIU Local 012"))
    ).otherwise(lit("")).alias("Union Name"),

    # ── Other identifiers ─────────────────────────────────────────────────
    concat(lit("CA"), lpad((col("id") + 5_000_000).cast("string"), 8, "0")).alias("C&A ID"),
    when(col("id") % 20 == 0, lit("true")).otherwise(lit("false")).alias("Is_NonEmp"),
)

print(f"Employee DataFrame ready: {TOTAL_RECORDS:,} records x {len(df_employees.columns)} columns")
print(f"Columns: {', '.join(df_employees.columns)}")
display(df_employees.limit(5))

# COMMAND ----------

# DBTITLE 1,Step 3 — Write Employee CSV Batches to ADLS
# ============================================================
# Step 3: Write 400K Employee Records as CSV Batches to ADLS
# Uses config: BATCH_SIZE, NUM_BATCHES, OUTPUT_FOLDER, FILE_PREFIX
# ============================================================

TEMP_PATH = f"{adls_base_path}/poc_output/_temp_emp_csv_batches"

print(f"Writing {TOTAL_RECORDS:,} records → {NUM_BATCHES} CSV files (≈{BATCH_SIZE:,} rows each)")
print(f"Destination : {OUTPUT_FOLDER}")
print("-" * 65)

# ---- Single Spark write pass, repartitioned into NUM_BATCHES files ----
(
    df_employees
    .repartition(NUM_BATCHES)
    .write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save(TEMP_PATH)
)

# ---- Collect and sort the part files written ----
part_files = sorted(
    [f.path for f in dbutils.fs.ls(TEMP_PATH) if f.name.startswith("part-")]
)
print(f"Part files written : {len(part_files)}")

# ---- Ensure output folder exists ----
try:
    dbutils.fs.mkdirs(OUTPUT_FOLDER)
except Exception:
    pass

# ---- Copy each part file with a clean, readable name ----
for i, part_file in enumerate(part_files):
    dest = f"{OUTPUT_FOLDER}{FILE_PREFIX}_{str(i).zfill(3)}.csv"
    dbutils.fs.cp(part_file, dest)
    print(f"  [{str(i).zfill(3)}] → {dest}")

# ---- Remove temp directory ----
dbutils.fs.rm(TEMP_PATH, recurse=True)

# ---- Final verification ----
print(f"\n{'='*65}")
csv_files  = [f for f in dbutils.fs.ls(OUTPUT_FOLDER) if f.name.endswith(".csv")]
total_mb   = sum(f.size for f in csv_files) / 1024 / 1024
print(f"CSV files in output folder : {len(csv_files)}")
print(f"Total size                 : {total_mb:.1f} MB")
print(f"Output folder              : {OUTPUT_FOLDER}")
print("\nBatch files ready for Staffbase ingestion:")
for f in sorted(csv_files, key=lambda x: x.name):
    print(f"  {f.name}  ({f.size / 1024 / 1024:.1f} MB)")

# COMMAND ----------

print("hii")

# COMMAND ----------

# DBTITLE 1,Step 4 — Staffbase CSV Import API (users/imports)
# ============================================================
# Step 4: Call Staffbase CSV Import API  POST /users/imports
# Multipart/form-data — reads each batch CSV from ADLS,
# stages it locally on the driver, then POSTs to Staffbase.
# ============================================================
import requests
import os
import time
from datetime import datetime

# ---- Auth & Endpoint ----
# The token from Staffbase Settings is Base64(clientId:clientSecret) — used as Basic Auth.
# This matches the same credentials already used in Cell 9 (PUT /api/posts).
# RECOMMENDED for production: store in Databricks Secret Scope:
# STAFFBASE_TOKEN = dbutils.secrets.get(scope="staffbase-scope", key="api-token")
STAFFBASE_TOKEN      = "NmEwMzhmMWExMGIwZGQ3Mzc5NDI0Nzk2OnZHSkR3NSYhS2hoXm4uS3pwJkZxfjR+WXFyTkg5TiktTmxiOylJaFRuelNfZC0wM2FUMHlbMDBWcVRdN0gpdX4="

# URL follows the same base pattern as Cell 9 (no /v1/)
# Cell 9 working example : https://krogertest.staffbase.com/api/posts/<id>
STAFFBASE_IMPORT_URL = "https://krogertest.staffbase.com/api/users/imports"

HEADERS = {
    # Basic Auth — Staffbase encodes clientId:clientSecret in Base64
    "Authorization": f"Basic {STAFFBASE_TOKEN}",
    "Accept":        "application/json",
    # NOTE: Do NOT set Content-Type here — requests sets it automatically
    #        for multipart/form-data (including the boundary value)
}

# ---- Retry config ----
MAX_RETRIES         = 3
RETRY_DELAY_SECONDS = 5
LOCAL_TMP           = "/tmp/staffbase_batch.csv"   # driver-local staging file

# ---- ADLS folders (set in Step 1 / Cell 10) ----
# Adjust these if your variables differ
try:
    _input = OUTPUT_FOLDER
except NameError:
    _input = f"{adls_base_path}/staffbase_input/"

processed_folder = f"{adls_base_path}/staffbase_processed/"
error_folder     = f"{adls_base_path}/staffbase_error/"

# ---- Ensure archive folders exist ----
for folder in [processed_folder, error_folder]:
    try:
        dbutils.fs.mkdirs(folder)
    except Exception:
        pass

# ---- Discover CSV batch files in ADLS input folder ----
csv_files = sorted(
    [f for f in dbutils.fs.ls(_input) if f.name.endswith(".csv")],
    key=lambda x: x.name
)
print(f"Found {len(csv_files)} CSV batch file(s) in: {_input}")
print("-" * 65)

# ---- Process each batch ----
summary    = []
import_ids = []   # stores {file, import_id} for use in Step 5 PATCH call

for batch_file in csv_files:
    adls_path  = batch_file.path
    fname      = batch_file.name
    attempt    = 0
    success    = False
    last_error = ""

    # --- Stage: copy from ADLS (abfss://) to driver local /tmp ---
    # dbutils.fs.cp with the "file://" prefix writes to the driver filesystem
    dbutils.fs.cp(adls_path, f"file://{LOCAL_TMP}", recurse=False)

    while attempt < MAX_RETRIES and not success:
        attempt += 1
        try:
            with open(LOCAL_TMP, "rb") as fh:
                resp = requests.post(
                    STAFFBASE_IMPORT_URL,
                    headers=HEADERS,
                    # ---- Import configuration parameters ----
                    # These mirror the settings selected in the manual UI:
                    # New Import → User Import → Update → UTF-8 → Comma
                    data={
                        "type":         "userImport",  # userImport | contactImport
                        "importType":   "update",      # update | overwriteAll
                        "textEncoding": "UTF-8",
                        "separator":    ",",           # comma separator
                    },
                    files={"file": (fname, fh, "text/csv")},
                    timeout=60
                )

            if resp.status_code == 201:
                success = True
                # Capture importId from Location response header
                # Staffbase returns: Location: /api/users/imports/{importId}
                location  = resp.headers.get("Location", "")
                import_id = location.rstrip("/").split("/")[-1] if location else None
                import_ids.append({"file": fname, "import_id": import_id})
                # Move to processed folder on success
                dbutils.fs.cp(adls_path, f"{processed_folder}{fname}")
                dbutils.fs.rm(adls_path)
                print(f"  [OK ] {fname}  → HTTP 201  |  importId: {import_id}  (attempt {attempt})")
            elif resp.status_code in (400, 401, 403):
                # Non-retryable errors
                last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
                print(f"  [ERR] {fname}  → {last_error}  (no retry)")
                break
            else:
                last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
                print(f"  [RTY] {fname}  → {last_error}  (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY_SECONDS)

        except Exception as e:
            last_error = str(e)
            print(f"  [EXC] {fname}  → {last_error}  (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY_SECONDS)

    if not success:
        # Move failed file to error folder
        try:
            dbutils.fs.cp(adls_path, f"{error_folder}{fname}")
            dbutils.fs.rm(adls_path)
        except Exception:
            pass

    summary.append({
        "file":        fname,
        "success":     success,
        "attempts":    attempt,
        "error":       "" if success else last_error,
        "processed_at": datetime.now().isoformat()
    })

# ---- Cleanup local staging file ----
try:
    os.remove(LOCAL_TMP)
except Exception:
    pass

# ---- Summary ----
print(f"\n{'='*65}")
ok  = sum(1 for s in summary if s["success"])
fail = len(summary) - ok
print(f"Total batches : {len(summary)}  |  Success : {ok}  |  Failed : {fail}")
print(f"Processed     : {processed_folder}")
print(f"Errors        : {error_folder}")

if summary:
    from pyspark.sql import Row
    summary_df = spark.createDataFrame([Row(**s) for s in summary])
    display(summary_df)

# COMMAND ----------

# DBTITLE 1,Step 4b — GET Profile Field IDs from Staffbase
# ============================================================
# Step 4b: GET /api/profile-fields
# Fetches all Staffbase profile field definitions for Kroger
# tenant. Run this ONCE to get the exact field IDs needed
# for the FIELD_MAPPING in Step 5.
# ============================================================
import requests, json

HDRS = {"Authorization": f"Basic {STAFFBASE_TOKEN}", "Accept": "application/json"}
IMPORT_ID = "6a3e66f7b2cbf06dafd3e159"   # latest importId

# ---------------------------------------------------------------
# A) GET existing import config — Staffbase auto-detects columns
#    from the uploaded CSV and builds a draft mapping.
#    This shows us the EXACT field IDs Staffbase expects.
# ---------------------------------------------------------------
print("=" * 65)
print("A) GET /api/users/imports/{id}/config  (auto-detected mapping)")
print("=" * 65)
r = requests.get(
    f"https://krogertest.staffbase.com/api/users/imports/{IMPORT_ID}/config",
    headers=HDRS, timeout=30
)
print(f"HTTP {r.status_code}")
if r.status_code == 200:
    print(json.dumps(r.json(), indent=2))
else:
    print(r.text)

# ---------------------------------------------------------------
# B) PATCH with ONLY standard documented fields (no custom).
#    If this returns 204, the issue is only with custom field IDs.
# ---------------------------------------------------------------
print("\n" + "=" * 65)
print("B) PATCH with standard fields ONLY  (isolate custom field issue)")
print("=" * 65)
minimal_payload = {
    "delta":      True,
    "removeAll":  False,
    "encoding":   "UTF-8",
    "separator":  ",",
    "type":       "USER",
    "mapping": {
        "externalId":              "Identifier",
        "eMail":                   "Email Address",
        "userName":                "Preferred Name",
        "status":                  "Employment Status",
        "profile-field:firstName": "First Name",
        "profile-field:lastName":  "Last Name",
        "profile-field:position":  "Job Title",
        "profile-field:department":"Department Code",
        "profile-field:location":  "Location",
    }
}
# Known working standard fields
base_mapping = {
    "externalId":              "Identifier",
    "eMail":                   "Email Address",
    "userName":                "Preferred Name",
    "status":                  "Employment Status",
    "profile-field:firstName": "First Name",
    "profile-field:lastName":  "Last Name",
    "profile-field:position":  "Job Title",
    "profile-field:department":"Department Code",
    "profile-field:location":  "Location",
}

# Custom fields — test TWO formats: camelCase vs exact display name
# The format that returns 204 is the correct one for Step 5
custom_fields_camel = {
    "profile-field:personNumber":          "Person Number",
    "profile-field:jobCode":               "Job Code",
    "profile-field:division":              "Division",
    "profile-field:businessDomain":        "Business Domain",
    "profile-field:district":              "District",
    "profile-field:jobFamily":             "Job Family",
    "profile-field:hrLocationCode":        "HR Location Code",
    "profile-field:hireDate":              "Hire Date",
    "profile-field:managerPersonNumber":   "Manager Person Number",
    "profile-field:executive":             "Executive",
    "profile-field:masterDepartment":      "Master Department",
    "profile-field:departmentDescription": "Department Description",
    "profile-field:union":                 "Union",
    "profile-field:unionName":             "Union Name",
    "profile-field:candAId":               "C&A ID",
    "profile-field:kxid":                  "kxid",
    "profile-field:Is_NonEmp":             "Is_NonEmp",
}

custom_fields_display = {
    "profile-field:Person Number":          "Person Number",
    "profile-field:Job Code":               "Job Code",
    "profile-field:Division":               "Division",
    "profile-field:Business Domain":        "Business Domain",
    "profile-field:District":               "District",
    "profile-field:Job Family":             "Job Family",
    "profile-field:HR Location Code":       "HR Location Code",
    "profile-field:Hire Date":              "Hire Date",
    "profile-field:Manager Person Number":  "Manager Person Number",
    "profile-field:Executive":              "Executive",
    "profile-field:Master Department":      "Master Department",
    "profile-field:Department Description": "Department Description",
    "profile-field:Union":                  "Union",
    "profile-field:Union Name":             "Union Name",
    "profile-field:C&A ID":                 "C&A ID",
    "profile-field:kxid":                   "kxid",
    "profile-field:Is_NonEmp":              "Is_NonEmp",
}

# ---------------------------------------------------------------
# C) GET /api/users  — fetch a real user object.
#    User profiles contain profile fields with their EXACT IDs.
#    Look for a 'profileFields' or 'customFields' key in the response.
# ---------------------------------------------------------------
print("\n" + "=" * 65)
print("C) GET /api/users  (find exact profile field IDs in user objects)")
print("=" * 65)

# ---- Find one of our imported users (externalId starts with KXI) ----
# and print their full profile to see the custom field keys
r3 = requests.get(
    "https://krogertest.staffbase.com/api/users?limit=50",
    headers=HDRS, timeout=30
)
print(f"HTTP {r3.status_code}")
if r3.status_code == 200:
    data  = r3.json()
    users = data if isinstance(data, list) else data.get("data", data.get("users", data.get("items", [])))
    print(f"Users returned: {len(users)}")

    # Find an imported user (has externalId starting with KXI)
    imported = [u for u in users if str(u.get("externalId","")).startswith("KXI")]
    admin    = [u for u in users if not str(u.get("externalId","")).startswith("KXI")]

    target = imported[0] if imported else (users[0] if users else None)
    label  = "imported KXI user" if imported else "first available user"

    if target:
        print(f"\n--- {label}: {target.get('externalId','N/A')} ---")
        # Show full profile object — custom field keys are here
        print("Full object:")
        print(json.dumps(target, indent=2))
else:
    print(r3.text)

print("\n" + "=" * 65)
print("D) All unique profile-field keys across users")
print("=" * 65)
if r3.status_code == 200:
    all_profile_keys = set()
    for u in users:
        all_profile_keys.update(u.get("profile", {}).keys())
    for k in sorted(all_profile_keys):
        print(f"  profile-field:{k}")

# ---------------------------------------------------------------
# E) Binary search — test confirmed vs inferred custom field groups
# ---------------------------------------------------------------
base = {
    "externalId": "Identifier", "eMail": "Email Address",
    "userName": "Preferred Name", "status": "Employment Status",
    "profile-field:firstName": "First Name", "profile-field:lastName": "Last Name",
    "profile-field:location": "Location",
}
confirmed_custom = {
    "profile-field:jobtitle":    "Job Title",
    "profile-field:hrlocationcode": "HR Location Code",
    "profile-field:district":    "District",
    "profile-field:division":    "Division",
    "profile-field:executive":   "Executive",
    "profile-field:hiredate":    "Hire Date",
    "profile-field:union":       "Union",
    "profile-field:isnonemp":    "Is_NonEmp",
    "profile-field:jobfamily_1": "Job Family",
    "profile-field:kxid":        "kxid",
}
inferred_custom = {
    "profile-field:personnumber":          "Person Number",
    "profile-field:jobcode":               "Job Code",
    "profile-field:businessdomain":        "Business Domain",
    "profile-field:managerpersonnumber":   "Manager Person Number",
    "profile-field:departmentcode":        "Department Code",
    "profile-field:masterdepartment":      "Master Department",
    "profile-field:departmentdescription": "Department Description",
    "profile-field:unionname":             "Union Name",
    "profile-field:candaid":               "C&A ID",
}

print("\n" + "=" * 65)
print("E) Binary search: confirmed vs inferred custom fields")
print("=" * 65)
# Confirmed fields ✔ (from GET /api/users) — skip these
# Test each inferred field INDIVIDUALLY to find the bad ID
print("\nTesting each inferred custom field individually:")
for key, csv_col in inferred_custom.items():
    p = {"delta": True, "encoding": "UTF-8", "separator": ",", "type": "USER",
         "mapping": {**base, **confirmed_custom, key: csv_col}}
    r = requests.patch(
        f"https://krogertest.staffbase.com/api/users/imports/{IMPORT_ID}/config",
        headers={**HDRS, "Content-Type": "application/json"}, json=p, timeout=30)
    icon = "✓" if r.status_code == 204 else "✗"
    print(f"  {icon} HTTP {r.status_code}  {key:<45} → CSV: {csv_col}")

# COMMAND ----------

# DBTITLE 1,Step 5 — PATCH: Configure Import + Field Mapping (users/imports/{id}/config)
# ============================================================
# Step 5: PATCH /api/users/imports/{importId}/config
# Configures import settings + maps CSV columns to Staffbase
# profile fields. HTTP 204 = configuration applied successfully
# and Staffbase will begin processing the import.
# ============================================================
import requests, json

# ---- importId ----
# Automatically picked from Step 4 (Location header capture).
# Falls back to the manually provided Postman importId.
try:
    IMPORT_ID = import_ids[-1]["import_id"] if import_ids else None
    if not IMPORT_ID:
        raise ValueError
except:
    IMPORT_ID = "6a3e53ee6035391e1c3afcd5"   # ← from Postman / last known upload

PATCH_URL = f"https://krogertest.staffbase.com/api/users/imports/{IMPORT_ID}/config"

PATCH_HEADERS = {
    "Authorization": f"Basic {STAFFBASE_TOKEN}",
    "Content-Type":  "application/json",
    "Accept":        "application/json",
}

# ---- Field mapping ----
# Key   = Staffbase profile field identifier
# Value = Exact CSV column header (must match Step 2 columns 1:1)
# Source: Verified from Staffbase Settings → Profile Fields screenshot
#
# Format rules:
#   - Standard system fields  : use key as-is  (externalId, email, status)
#   - Standard profile fields : prefix profile-field: + camelCase
#   - Custom profile fields   : prefix profile-field: + camelCase of display name
# ---- Get custom field IDs (run once, paste IDs into FIELD_MAPPING below) ----
# import requests
# r = requests.get(
#     "https://krogertest.staffbase.com/api/profile-fields",
#     headers={"Authorization": f"Basic {STAFFBASE_TOKEN}", "Accept": "application/json"}
# )
# print(r.json())   # look for "id" next to each field display name

# All 26 field IDs fully resolved and tested:
# ✔ Confirmed via GET /api/users : hrlocationcode, district, division, executive,
#                                    hiredate, union, isnonemp, jobfamily_1, jobtitle, kxid
# ✔ Confirmed via individual PATCH: personnumber, jobcode, businessdomain,
#                                    managerpersonnumber, departmentcode, masterdepartment,
#                                    departmentdescription, unionname
# ✔ C&A ID → caid  (Staffbase stripped "&" and space from display name)
FIELD_MAPPING = {
    # ── Standard system fields ──────────────────────────────────────────
    "externalId":                          "Identifier",
    "eMail":                               "Email Address",
    "userName":                            "Preferred Name",
    "status":                              "Employment Status",  # activated / deactivated

    # ── Standard built-in profile fields ───────────────────────────────
    "profile-field:firstName":             "First Name",
    "profile-field:lastName":              "Last Name",
    "profile-field:location":              "Location",

    # ── Custom fields — ALL IDs verified via API ─────────────────────────
    "profile-field:jobtitle":              "Job Title",
    "profile-field:hrlocationcode":        "HR Location Code",
    "profile-field:district":              "District",
    "profile-field:division":              "Division",
    "profile-field:executive":             "Executive",
    "profile-field:hiredate":              "Hire Date",
    "profile-field:union":                 "Union",
    "profile-field:isnonemp":              "Is_NonEmp",
    "profile-field:jobfamily_1":           "Job Family",
    "profile-field:kxid":                  "kxid",
    "profile-field:personnumber":          "Person Number",
    "profile-field:jobcode":               "Job Code",
    "profile-field:businessdomain":        "Business Domain",
    "profile-field:managerpersonnumber":   "Manager Person Number",
    "profile-field:departmentcode":        "Department Code",
    "profile-field:masterdepartment":      "Master Department",
    "profile-field:departmentdescription": "Department Description",
    "profile-field:unionname":             "Union Name",
    "profile-field:caid":                  "C&A ID",       # & stripped by Staffbase → caid
}

# ---- PATCH payload ----
# ⚠️  delta and removeAll are MUTUALLY EXCLUSIVE — use one or the other:
#     delta=True   → Update mode: create new + update existing, preserve users NOT in CSV
#     removeAll=True → Overwrite mode: create + update + REMOVE users not in CSV
# ⚠️  separator must be "," — Staffbase auto-detects ";" from prior uploads;
#     setting explicitly ensures our Spark comma-CSV is parsed correctly.
payload = {
    "delta":                 True,    # Update mode (do NOT use with removeAll)
    "encoding":              "UTF-8",
    "separator":             ",",     # our Spark CSV uses comma
    "type":                  "USER",
    "generateRecoveryCodes": False,
    "sendMailsNew":          False,   # no invite emails during POC
    "sendMailsPending":      False,
    "mapping":               FIELD_MAPPING,
}

print(f"PATCH  → {PATCH_URL}")
print(f"Import ID : {IMPORT_ID}")
print(f"Mapping   : {len(FIELD_MAPPING)} fields")
print("-" * 65)

resp = requests.patch(PATCH_URL, headers=PATCH_HEADERS, json=payload, timeout=30)

print(f"HTTP {resp.status_code}")

if resp.status_code == 204:
    print("Import configuration applied — Staffbase is now processing the import.")
    print(f"Monitor at : https://krogertest.staffbase.com/studio/users/import")
elif resp.status_code == 400:
    print(f"Validation error — check field mapping keys:\n{resp.text}")
elif resp.status_code == 401:
    print("Auth failed — check STAFFBASE_TOKEN")
elif resp.status_code == 404:
    print(f"importId not found: {IMPORT_ID}")
    print("Re-run Step 3 + Step 4 to upload a fresh CSV and get a new importId.")
else:
    print(f"Unexpected: {resp.status_code} — {resp.text}")

# COMMAND ----------

# DBTITLE 1,Step 6 — PUT: Trigger Import (set state to IMPORT_PENDING)
# ============================================================
# Step 6: PUT /api/users/imports/{importId}
# Sets the import state to IMPORT_PENDING to actually trigger
# processing. This is the final step that creates/updates users.
#
# State flow:
#   DRAFT → PREVIEW_PENDING → CREATING_PREVIEW → PREVIEW_READY
#        → IMPORT_PENDING → IMPORTING → COMPLETED
#
# IMPORT_PENDING can be set when current state is:
#   PREVIEW_READY | IMPORT_PENDING | DRAFT | CANCELLED
# ============================================================
import requests, json, time

# ---- importId (reuse from Step 5) ----
try:
    IMPORT_ID = import_ids[-1]["import_id"] if import_ids else None
    if not IMPORT_ID:
        raise ValueError
except:
    IMPORT_ID = "6a3e66f7b2cbf06dafd3e159"   # ← latest known importId

PUT_URL = f"https://krogertest.staffbase.com/api/users/imports/{IMPORT_ID}"

PUT_HEADERS = {
    "Authorization": f"Basic {STAFFBASE_TOKEN}",
    "Content-Type":  "application/json",
    "Accept":        "application/json",
}

# ---- Step 6a: Check current import state before triggering ----
print("Checking current import state...")
current = requests.get(PUT_URL, headers=PUT_HEADERS, timeout=30)
if current.status_code == 200:
    state = current.json().get("state", "UNKNOWN")
    print(f"  Current state : {state}")
else:
    print(f"  Could not fetch state: HTTP {current.status_code}  {current.text[:200]}")
    state = "UNKNOWN"

# ---- Step 6b: Set state to IMPORT_PENDING ----
print(f"\nPUT → {PUT_URL}")
print(f"Payload : {{\"state\": \"IMPORT_PENDING\"}}")
print("-" * 65)

# Docs say PUT but actual API only allows PATCH on this endpoint (405 on PUT)
resp = requests.patch(
    PUT_URL,
    headers=PUT_HEADERS,
    json={"state": "IMPORT_PENDING"},
    timeout=30
)

print(f"HTTP {resp.status_code}")

if resp.status_code == 204:
    print("✓ Import triggered successfully! State set to IMPORT_PENDING.")
    print("  Staffbase is now creating/updating users from the CSV.")
    print(f"  Monitor progress: https://krogertest.staffbase.com/studio/users/import")

    # ---- Step 6c: Poll import state until COMPLETED or CANCELLED ----
    print("\nPolling import status...")
    for i in range(12):   # poll up to 2 minutes (12 x 10s)
        time.sleep(10)
        poll = requests.get(PUT_URL, headers=PUT_HEADERS, timeout=30)
        if poll.status_code == 200:
            data  = poll.json()
            state = data.get("state", "UNKNOWN")
            total = data.get("total",   "?")
            done  = data.get("done",    "?")
            fails = data.get("failed",  "?")
            print(f"  [{i+1:02d}] State: {state:<20} Total: {total}  Done: {done}  Failed: {fails}")
            if state in ("COMPLETED", "CANCELLED"):
                break
        else:
            print(f"  [{i+1:02d}] Poll failed: HTTP {poll.status_code}")
            break

    print(f"\nFinal state : {state}")
    if state == "COMPLETED":
        print("✓ Import COMPLETED — check users at: https://krogertest.staffbase.com/studio/users")
    elif state == "CANCELLED":
        print("✗ Import was CANCELLED by Staffbase.")
    else:
        print("  Import still in progress — check Staffbase portal for final status.")

elif resp.status_code == 401:
    print("✗ Auth failed — check STAFFBASE_TOKEN")
elif resp.status_code == 404:
    print(f"✗ importId not found: {IMPORT_ID}")
    print("  Re-run Steps 3 → 4 → 5 to create a fresh import entry.")
else:
    print(f"✗ Unexpected: HTTP {resp.status_code} — {resp.text}")

# COMMAND ----------

# DBTITLE 1,Step 7 — GET: Stream Import Errors (users/imports/{id}/errors)
# ============================================================
# Step 7: GET /api/users/imports/{importId}/errors
# Returns all error records from the import in CSV format.
# Run this after Step 6 completes to check for any failed rows.
# ============================================================
import requests, io
import pandas as pd

# ---- importId (reuse from Step 6) ----
try:
    IMPORT_ID = import_ids[-1]["import_id"] if import_ids else None
    if not IMPORT_ID:
        raise ValueError
except:
    IMPORT_ID = "6a3e66f7b2cbf06dafd3e159"   # ← latest known importId

ERRORS_URL = f"https://krogertest.staffbase.com/api/users/imports/{IMPORT_ID}/errors"

ERR_HEADERS = {
    "Authorization": f"Basic {STAFFBASE_TOKEN}",
    "Accept":        "text/csv, application/json",
}

print(f"GET → {ERRORS_URL}")
print(f"Import ID : {IMPORT_ID}")
print("-" * 65)

resp = requests.get(ERRORS_URL, headers=ERR_HEADERS, timeout=30)

print(f"HTTP {resp.status_code}")

if resp.status_code == 200:
    content = resp.text.strip()

    if not content:
        print("✓ No errors found — all records imported successfully!")
    else:
        # Parse CSV response into a pandas DataFrame for display
        try:
            err_df = pd.read_csv(io.StringIO(content))
            print(f"⚠️  {len(err_df)} error record(s) found:\n")
            display(err_df)

            # Also save errors to ADLS for audit trail
            err_path = f"{adls_base_path}/staffbase_error/import_errors_{IMPORT_ID}.csv"
            spark.createDataFrame(err_df).write \
                .format("csv") \
                .option("header", "true") \
                .mode("overwrite") \
                .save(err_path)
            print(f"\nError log saved to : {err_path}")

        except Exception as e:
            # Fallback: print raw CSV if parsing fails
            print(f"Could not parse as DataFrame ({e}). Raw response:")
            print(content[:3000])

elif resp.status_code == 401:
    print("✗ Auth failed — check STAFFBASE_TOKEN")
elif resp.status_code == 404:
    print(f"✗ Import ID not found: {IMPORT_ID}")
else:
    print(f"✗ Unexpected: HTTP {resp.status_code} — {resp.text[:300]}")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import rand, round, concat, lit, col

# ============================================
# Step 2: Create a Random DataFrame
# ============================================
df = spark.range(100).select(
    col("id").alias("employee_id"),
    concat(lit("emp_"), col("id")).alias("name"),
    (rand() * 50 + 20).cast("int").alias("age"),
    round(rand() * 100000 + 30000, 2).alias("salary")
)

print("Random DataFrame created with 100 rows:")
display(df)

# COMMAND ----------

# DBTITLE 1,Write DataFrame as JSON to ADLS
# ============================================
# Step 3: Write DataFrame as JSON to ADLS via direct ABFSS path
# ============================================
adls_base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
output_path = f"{adls_base_path}/poc_output/employee_data_json"

df.write.format("json").mode("overwrite").save(output_path)

print(f"Successfully written JSON to: {output_path}")

# Verify files were written
display(dbutils.fs.ls(output_path))

# COMMAND ----------

# DBTITLE 1,POC — Staffbase PUT API with 100 Sample Records from ADLS
# ============================================================
# POC — Call Staffbase PUT API with 100 Sample Records from ADLS
# + staffbase_exposed flag + Actual API Response + Delta Table
# Endpoint : PUT /api/posts/6a34f73e8ca631260ef03dc0
# Auth     : Basic Auth (Username + Password)
# ============================================================
import requests
import pandas as pd
from datetime import datetime

# ---- Staffbase Auth & Endpoint ----
STAFFBASE_USERNAME = "6a038f1a10b0dd7379424796"
STAFFBASE_PASSWORD = "vGJDw5&!Khh^n.Kzp&Fq~4~YqrNH9N)-Nlb;)IhTnzS_d-03aT0y[00VqT]7H)u~"
STAFFBASE_ENDPOINT = "https://krogertest.staffbase.com/api/posts/6a34f73e8ca631260ef03dc0"

HEADERS = {
    "Content-Type": "application/json",
    "Accept":       "application/json"
}

# Delta table path on ADLS
DELTA_TABLE_PATH = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/poc_output/staffbase_api_log_delta"

# ---- Read 100 Sample Records from ADLS ----
sample_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/poc_output/employee_data_json"
sample_df   = spark.read.format("json").load(sample_path).limit(100).toPandas()
print(f"Records loaded from ADLS : {len(sample_df)}")
print("-" * 60)

# ---- Call Staffbase PUT API for each record ----
results    = []
called_at  = datetime.now().isoformat()

for _, row in sample_df.iterrows():
    payload = {
        "contents": {
            "en_US": {
                "image":  "https://cdn.qumucloud.com/asset/kroger-sbx.qumucloud.com/6X1uKGKZ5rEW1ft4PNTBar",
                "teaser": f"Employee: {row.get('name', 'N/A')} | ID: {row.get('employee_id', 'N/A')}"
            }
        },
        "notificationChannels": ["email", "push"]
    }

    try:
        response = requests.put(
            STAFFBASE_ENDPOINT,
            headers=HEADERS,
            json=payload,
            auth=requests.auth.HTTPBasicAuth(STAFFBASE_USERNAME, STAFFBASE_PASSWORD),
            timeout=30
        )
        staffbase_exposed = response.status_code in (200, 201, 202)

        # Capture actual API response body from Staffbase
        try:
            api_response = str(response.json())[:1000]
        except Exception:
            api_response = response.text[:1000]

        results.append({
            "employee_id":       str(row.get("employee_id", "")),
            "name":              str(row.get("name", "")),
            "age":               int(row.get("age", 0)),
            "salary":            float(row.get("salary", 0.0)),
            "http_status":       response.status_code,
            "staffbase_exposed": staffbase_exposed,   # ← FLAG COLUMN
            "api_response":      api_response,         # ← ACTUAL STAFFBASE RESPONSE
            "api_called_at":     called_at
        })
        icon = "✓" if staffbase_exposed else "✗"
        print(f"  Record {str(row.get('employee_id','')):>4} : {icon} HTTP {response.status_code} | staffbase_exposed = {staffbase_exposed}")

    except Exception as e:
        results.append({
            "employee_id":       str(row.get("employee_id", "")),
            "name":              str(row.get("name", "")),
            "age":               int(row.get("age", 0)),
            "salary":            float(row.get("salary", 0.0)),
            "http_status":       0,
            "staffbase_exposed": False,
            "api_response":      str(e),
            "api_called_at":     called_at
        })
        print(f"  Record {str(row.get('employee_id','')):>4} : ✗ ERROR — {str(e)}")

# ---- Build Spark DataFrame ----
results_spark = spark.createDataFrame(pd.DataFrame(results))

# ---- Save as Delta Table to ADLS ----
results_spark.write.format("delta").mode("overwrite").save(DELTA_TABLE_PATH)
print(f"\nDelta table saved  : {DELTA_TABLE_PATH}")

# ---- Summary ----
success_count = sum(1 for r in results if r["staffbase_exposed"])
failed_count  = len(results) - success_count
print(f"Total              : {len(results)}")
print(f"staffbase_exposed  : {success_count} True  |  {failed_count} False")
print("-" * 60)

# ---- Display Final Table with staffbase_exposed flag ----
display(results_spark.select("employee_id", "name", "age", "salary", "http_status", "staffbase_exposed", "api_response", "api_called_at"))

# COMMAND ----------

# DBTITLE 1,Staffbase API — Configuration
# ============================================================
# STAFFBASE API INTEGRATION — CONFIGURATION
# Full Load: 400K records via CSV batches from ADLS
# ============================================================
import requests
import json
import time
from datetime import datetime

# ---- ADLS Path Configuration ----
storage_account_name = "staffbaseadlspoc"
container_name       = "raw"
adls_base_path       = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"

input_folder         = f"{adls_base_path}/staffbase_input/"       # Drop CSV files here
processed_folder     = f"{adls_base_path}/staffbase_processed/"   # Moved after success
error_folder         = f"{adls_base_path}/staffbase_error/"       # Moved on failure

# ---- Staffbase API Configuration ----
# RECOMMENDED: Use Databricks Secret Scope (ask your Staffbase admin for the token)
# staffbase_api_token = dbutils.secrets.get(scope="staffbase-scope", key="api-token")
staffbase_api_token  = "<YOUR_STAFFBASE_API_TOKEN>"               # Replace with actual token
staffbase_base_url   = "https://<your-tenant>.staffbase.com/api/v1"  # Replace with your tenant URL
staffbase_endpoint   = f"{staffbase_base_url}/users"              # Adjust endpoint: /users, /content, etc.

# ---- Batch & Retry Configuration ----
BATCH_SIZE           = 500    # Records per API call (tune per Staffbase rate limit)
MAX_RETRIES          = 3      # Retry attempts per failed batch
RETRY_DELAY_SECONDS  = 5      # Wait (seconds) between retries

print("Configuration loaded.")
print(f"  Input path   : {input_folder}")
print(f"  API Endpoint : {staffbase_endpoint}")
print(f"  Batch Size   : {BATCH_SIZE} records/call")

# COMMAND ----------

# DBTITLE 1,Staffbase API — Helper Functions
# ============================================================
# HELPER FUNCTIONS
# ============================================================

# ---- 1. List unprocessed CSV files in ADLS ----
def get_pending_files(input_path, processed_path):
    """Return CSV files in input_path not yet in processed_path."""
    try:
        all_files = [f.path for f in dbutils.fs.ls(input_path) if f.name.endswith(".csv")]
    except Exception:
        print(f"Input folder not found or empty: {input_path}")
        return []
    try:
        done_files = {f.name for f in dbutils.fs.ls(processed_path)}
    except Exception:
        done_files = set()
    pending = [f for f in all_files if f.split("/")[-1] not in done_files]
    print(f"Pending files: {len(pending)} of {len(all_files)} total")
    return pending


# ---- 2. Read CSV from ADLS into Pandas DataFrame ----
def read_csv_from_adls(file_path):
    """Read a CSV file from ADLS using Spark, convert to pandas."""
    spark_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path)
    )
    return spark_df.toPandas()


# ---- 3. Split DataFrame into batches ----
def chunk_dataframe(df, batch_size):
    """Yield successive batch_size chunks from DataFrame."""
    for i in range(0, len(df), batch_size):
        yield df.iloc[i : i + batch_size]


# ---- 4. Call Staffbase API with retry logic ----
def call_staffbase_api(batch_df, token, endpoint):
    """
    POST a batch of records to the Staffbase API.
    Returns (success: bool, http_status: int, message: str)
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json"
    }
    # Convert batch to list of dicts for JSON payload
    # Adjust the payload structure based on Staffbase API requirements
    payload = {"users": batch_df.to_dict(orient="records")}  # Adjust key as per API spec

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        if response.status_code in (200, 201, 202):
            return True, response.status_code, "Success"
        else:
            return False, response.status_code, response.text[:300]
    except requests.exceptions.RequestException as e:
        return False, 0, str(e)


# ---- 5. Move file between ADLS folders ----
def move_file(src_path, dest_folder):
    """Copy file to destination and remove source."""
    file_name = src_path.split("/")[-1]
    dest_path = f"{dest_folder}{file_name}"
    dbutils.fs.cp(src_path, dest_path)
    dbutils.fs.rm(src_path)
    print(f"  Moved: {file_name}  →  {dest_folder.split('/')[-2]}")


print("Helper functions defined.")

# COMMAND ----------

# DBTITLE 1,Staffbase API — Main Orchestration
# ============================================================
# MAIN ORCHESTRATION — Detect Files → Read CSV → Call Staffbase API
# ============================================================

def process_all_files():
    pending_files = get_pending_files(input_folder, processed_folder)
    if not pending_files:
        print("No new files to process. Exiting.")
        return []

    total_success = 0
    total_failed  = 0
    run_log       = []

    for file_path in pending_files:
        file_name = file_path.split("/")[-1]
        print(f"\n{'='*65}")
        print(f"Processing : {file_name}")
        print(f"{'='*65}")

        try:
            # Step 1: Read CSV from ADLS
            df            = read_csv_from_adls(file_path)
            total_records = len(df)
            print(f"  Records loaded : {total_records:,}")

            file_success = 0
            file_failed  = 0

            # Step 2: Process in batches & call Staffbase API
            batches = list(chunk_dataframe(df, BATCH_SIZE))
            print(f"  Batches        : {len(batches)} (size={BATCH_SIZE})")

            for batch_num, batch_df in enumerate(batches, start=1):
                batch_count = len(batch_df)
                print(f"  Batch {batch_num:03d}/{len(batches):03d}: {batch_count} records ", end="")

                success = False
                for attempt in range(1, MAX_RETRIES + 1):
                    ok, status, msg = call_staffbase_api(batch_df, staffbase_api_token, staffbase_endpoint)
                    if ok:
                        print(f"✓ (HTTP {status})")
                        file_success += batch_count
                        success = True
                        break
                    else:
                        print(f"\n    Attempt {attempt}/{MAX_RETRIES} failed — HTTP {status}: {msg}")
                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY_SECONDS)

                if not success:
                    print(f"  !! Batch {batch_num} FAILED after {MAX_RETRIES} attempts.")
                    file_failed += batch_count

            # Step 3: Move file to processed or error folder
            dest = processed_folder if file_failed == 0 else error_folder
            status_label = "COMPLETED" if file_failed == 0 else "PARTIAL_ERROR"
            move_file(file_path, dest)

        except Exception as e:
            print(f"  CRITICAL ERROR: {str(e)}")
            move_file(file_path, error_folder)
            total_records = 0
            file_success  = 0
            file_failed   = 0
            status_label  = "ERROR"

        total_success += file_success
        total_failed  += file_failed
        run_log.append({
            "file":      file_name,
            "total":     total_records,
            "success":   file_success,
            "failed":    file_failed,
            "status":    status_label,
            "timestamp": datetime.now().isoformat()
        })

    # ---- Run Summary ----
    print(f"\n{'='*65}")
    print("RUN SUMMARY")
    print(f"{'='*65}")
    for entry in run_log:
        print(f"  {entry['file']:<40} | {entry['status']:<14} | Success: {entry['success']:>6,} | Failed: {entry['failed']:>6,}")
    print(f"\n  Total Sent OK : {total_success:,}")
    print(f"  Total Failed  : {total_failed:,}")
    return run_log

# ---- EXECUTE ----
run_results = process_all_files()

# COMMAND ----------

# DBTITLE 1,Direct API for Delta
# ============================================================
# Direct API for Delta
# Delta Load: Records pushed directly to Staffbase via PUT API
# No CSV/ADLS file involved — records arrive as in-memory events
# Endpoint : PUT /api/posts/6a34f73e8ca631260ef03dc0
# Auth     : Basic Auth
# ============================================================
import requests
from datetime import datetime

# ---- Staffbase Auth & Endpoint (same as Full Load POC) ----
STAFFBASE_USERNAME = "6a038f1a10b0dd7379424796"
STAFFBASE_PASSWORD = "vGJDw5&!Khh^n.Kzp&Fq~4~YqrNH9N)-Nlb;)IhTnzS_d-03aT0y[00VqT]7H)u~"
STAFFBASE_ENDPOINT = "https://krogertest.staffbase.com/api/posts/6a34f73e8ca631260ef03dc0"

HEADERS = {
    "Content-Type": "application/json",
    "Accept":       "application/json"
}

# ---- 2-3 Sample Delta Records (simulating incremental change feed) ----
# In production, these would come from a CDC/streaming source or change table
delta_records = [
    {"employee_id": 1001, "name": "emp_1001", "age": 34, "salary": 72000.00, "change_type": "UPDATE"},
    {"employee_id": 1002, "name": "emp_1002", "age": 28, "salary": 55000.00, "change_type": "INSERT"},
    {"employee_id": 1003, "name": "emp_1003", "age": 41, "salary": 91500.50, "change_type": "UPDATE"},
]

print(f"Delta records to process : {len(delta_records)}")
print("-" * 60)

# ---- Call Staffbase PUT API directly for each delta record ----
delta_results = []
called_at = datetime.now().isoformat()

for record in delta_records:
    payload = {
        "contents": {
            "en_US": {
                "image":  "https://cdn.qumucloud.com/asset/kroger-sbx.qumucloud.com/6X1uKGKZ5rEW1ft4PNTBar",
                "teaser": f"[{record['change_type']}] Employee: {record['name']} | ID: {record['employee_id']}"
            }
        },
        "notificationChannels": ["email", "push"]
    }

    try:
        response = requests.put(
            STAFFBASE_ENDPOINT,
            headers=HEADERS,
            json=payload,
            auth=requests.auth.HTTPBasicAuth(STAFFBASE_USERNAME, STAFFBASE_PASSWORD),
            timeout=30
        )
        staffbase_exposed = response.status_code in (200, 201, 202)

        try:
            api_response = str(response.json())[:500]
        except Exception:
            api_response = response.text[:500]

        delta_results.append({
            "employee_id":       record["employee_id"],
            "name":              record["name"],
            "change_type":       record["change_type"],
            "http_status":       response.status_code,
            "staffbase_exposed": staffbase_exposed,
            "api_response":      api_response,
            "api_called_at":     called_at
        })
        icon = "✓" if staffbase_exposed else "✗"
        print(f"  [{record['change_type']:<6}] Employee {record['employee_id']} : {icon} HTTP {response.status_code} | staffbase_exposed = {staffbase_exposed}")

    except Exception as e:
        delta_results.append({
            "employee_id":       record["employee_id"],
            "name":              record["name"],
            "change_type":       record["change_type"],
            "http_status":       0,
            "staffbase_exposed": False,
            "api_response":      str(e),
            "api_called_at":     called_at
        })
        print(f"  [{record['change_type']:<6}] Employee {record['employee_id']} : ✗ ERROR — {str(e)}")

# ---- Summary ----
print("-" * 60)
success_count = sum(1 for r in delta_results if r["staffbase_exposed"])
print(f"Delta records processed  : {len(delta_results)}")
print(f"Successfully exposed      : {success_count}")
print(f"Failed                   : {len(delta_results) - success_count}")

# ---- Display Results as Spark DataFrame ----
import pandas as pd
delta_spark = spark.createDataFrame(pd.DataFrame(delta_results))
display(delta_spark.select("employee_id", "name", "change_type", "http_status", "staffbase_exposed", "api_response", "api_called_at"))

# COMMAND ----------

# DBTITLE 1,Run Summary as Spark DataFrame
# ============================================================
# DISPLAY RUN LOG AS STRUCTURED TABLE
# ============================================================
if run_results:
    import pandas as pd
    summary_df = spark.createDataFrame(pd.DataFrame(run_results))
    display(summary_df)
else:
    print("No files were processed in this run.")
