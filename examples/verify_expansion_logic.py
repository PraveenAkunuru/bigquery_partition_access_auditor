
from bq_partition_audit import SQLGlotExtractor, DimensionFilter

def verify_user_scenario():
    sql = """
    WITH frequent_ss_items AS (
        SELECT item_sk FROM store_sales 
        JOIN date_dim d ON ss_sold_date_sk = d_date_sk
        WHERE d.d_year = 2002 AND d.d_moy = 11
    )
    SELECT * FROM frequent_ss_items
    """
    
    print(f"Analyzing SQL Pattern:\n{sql}")
    
    extractor = SQLGlotExtractor()
    # We parse looking for partition keys, but for expansion we care about the 'dims' return
    _, dims = extractor.extract_with_context(sql, "ss_sold_date_sk")
    
    print("\nDetected Dimension Candidates:")
    found_year = False
    found_moy = False
    
    for d in dims:
        print(f" - Table: {d.table_alias}, Col: {d.column}, Op: {d.operator}, Val: {d.value}")
        if d.column == "d_year" and d.value == "2002": found_year = True
        if d.column == "d_moy" and d.value == "11": found_moy = True
        
    if found_year and found_moy:
        print("\nSUCCESS: User scenario (Year/Moy) correctly detected.")
    else:
        print("\nFAILURE: Failed to detect Year/Moy filters.")

if __name__ == "__main__":
    verify_user_scenario()
