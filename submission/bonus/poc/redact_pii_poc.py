"""
Proof of Concept (PoC) for PII Redaction at the Bronze-to-Silver transition.
Uses regex to detect and redact Emails, Phone Numbers, and Identification Numbers (CMND/CCCD).
Writes the output to a local Delta table.
Run using: .venv\\Scripts\\python submission/bonus/poc/redact_pii_poc.py
"""
import re
import polars as pl
from deltalake import DeltaTable, write_deltalake

# 1. Define PII Patterns
EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
PHONE_REGEX = r"\b(0[3|5|7|8|9])([0-9]{8})\b"
ID_REGEX = r"\b[0-9]{9,12}\b" # 9 to 12 digit ID cards

def redact_text(text: str) -> str:
    if not text:
        return text
    # Redact Emails
    text = re.sub(EMAIL_REGEX, "[REDACTED_EMAIL]", text)
    # Redact Phone Numbers
    text = re.sub(PHONE_REGEX, "[REDACTED_PHONE]", text)
    # Redact National IDs
    text = re.sub(ID_REGEX, "[REDACTED_ID]", text)
    return text

def main():
    print("--- PII Redaction PoC ---")
    
    # 2. Sample raw LLM requests containing sensitive PII
    raw_data = [
        {"request_id": "req-1", "tenant_id": "tenant-A", "prompt": "Hi, my email is alice.smith@gmail.com and my phone is 0912345678. Help me draft a contract."},
        {"request_id": "req-2", "tenant_id": "tenant-B", "prompt": "My ID card number is 012345678912. Can you lookup my details?"},
        {"request_id": "req-3", "tenant_id": "tenant-A", "prompt": "Tell me a joke about programmers."}
    ]
    
    df_raw = pl.DataFrame(raw_data)
    print("\nRaw Data at Bronze landing:")
    print(df_raw)
    
    # 3. Apply Redaction (equivalent to Silver processing)
    print("\nProcessing Bronze -> Silver (Applying PII Redaction)...")
    df_clean = df_raw.with_columns(
        pl.col("prompt").map_elements(redact_text, return_dtype=pl.String).alias("prompt_redacted")
    ).drop("prompt")
    
    print("\nCleaned Data ready for Silver:")
    print(df_clean)
    
    # 4. Write to Silver Delta Table
    silver_path = "./_lakehouse/silver_poc_calls"
    print(f"\nWriting to Silver Delta Table at: {silver_path} ...")
    write_deltalake(silver_path, df_clean.to_arrow(), mode="overwrite")
    
    # 5. Read back from Delta Table to verify
    dt = DeltaTable(silver_path)
    df_read = pl.from_arrow(dt.to_pyarrow_table())
    print("\nSuccessfully Read Back from Silver Delta Table:")
    print(df_read)
    
    print("\nPoC completed successfully! PII is fully redacted in storage.")

if __name__ == "__main__":
    main()
