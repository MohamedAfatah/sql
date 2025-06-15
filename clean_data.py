import re
import os

def clean_sql_data(input_file, output_file, table_name="CombinedAddons"):
    """
    Clean MySQL INSERT data for SQL Server compatibility
    Supports multiple tables: CombinedAddons, CombinedBio, etc.
    """
    
    print(f"Reading data from: {input_file}")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            content = file.read()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found!")
        return False
    except Exception as e:
        print(f"Error reading file: {e}")
        return False
    
    print("Cleaning data...")
    
    # Step 1: Extract INSERT statements and detect table
    def detect_table_and_columns(content):
        """Detect table name and column structure from INSERT statement"""
        
        # Look for INSERT INTO statement
        insert_match = re.search(r'INSERT INTO `?(\w+)`?\s*\(([^)]+)\)', content, re.IGNORECASE)
        
        if insert_match:
            detected_table = insert_match.group(1)
            columns_raw = insert_match.group(2)
            columns = [col.strip().strip('`') for col in columns_raw.split(',')]
            
            # Convert to PascalCase
            table_pascal = ''.join(word.capitalize() for word in detected_table.split('_'))
            
            return table_pascal, columns
        
        # Fallback to provided table name
        return table_name, []
    
    # Step 2: Fix single quotes and special characters
    def fix_quotes_and_special_chars(text):
        """Fix single quotes and special characters for SQL Server"""
        
        # Replace single quotes within string literals
        def replace_quotes_in_string(match):
            content = match.group(1)
            # Fix single quotes by doubling them
            content = content.replace("'", "''")
            return f"'{content}'"
        
        # Pattern to match string literals (content between single quotes)
        # This handles nested quotes and escaped quotes
        pattern = r"'((?:[^'\\]|\\.|'')*?)'"
        text = re.sub(pattern, replace_quotes_in_string, text)
        
        return text
    
    # Step 3: Fix common apostrophe and contraction issues
    def fix_apostrophes(text):
        """Fix common apostrophe issues in descriptions"""
        
        apostrophe_fixes = {
            # Contractions
            "don't": "don''t", "won't": "won''t", "can't": "can''t",
            "isn't": "isn''t", "aren't": "aren''t", "wasn't": "wasn''t",
            "weren't": "weren''t", "doesn't": "doesn''t", "haven't": "haven''t",
            "hasn't": "hasn''t", "hadn't": "hadn''t", "wouldn't": "wouldn''t",
            "shouldn't": "shouldn''t", "couldn't": "couldn''t",
            
            # Possessives and combinations
            "it's": "it''s", "that's": "that''s", "what's": "what''s",
            "here's": "here''s", "there's": "there''s", "where's": "where''s",
            "let's": "let''s", "who's": "who''s",
            
            # Future tense
            "you'll": "you''ll", "we'll": "we''ll", "they'll": "they''ll",
            "I'll": "I''ll", "he'll": "he''ll", "she'll": "she''ll",
            
            # Past tense
            "you're": "you''re", "we're": "we''re", "they're": "they''re",
            "I'm": "I''m", "you've": "you''ve", "we've": "we''ve",
            "they've": "they''ve", "I've": "I''ve"
        }
        
        for original, replacement in apostrophe_fixes.items():
            text = text.replace(original, replacement)
        
        return text
    
    # Step 4: Handle newlines and special formatting
    def fix_newlines_and_formatting(text):
        """Fix newlines and special formatting for SQL Server"""
        
        # Convert literal \n to actual newlines, then back to SQL Server format
        text = text.replace('\\n', '\n')
        
        # Replace actual newlines with spaces or keep them as literal \n for SQL
        # For SQL Server, we'll convert them to CHAR(13)+CHAR(10) or just spaces
        text = text.replace('\n', ' ')  # Convert newlines to spaces for simplicity
        
        # Fix multiple spaces
        text = re.sub(r'\s+', ' ', text)
        
        # Fix copyright and special symbols
        text = text.replace('©', '(c)')
        text = text.replace('®', '(R)')
        text = text.replace('™', '(TM)')
        
        return text
    
    # Step 5: Fix specific business-related terms
    def fix_business_terms(text):
        """Fix specific business terms and company names"""
        
        # Company names with special characters
        text = text.replace("e&", "e&")  # Keep e& as is
        text = text.replace("UAE's", "UAE''s")
        
        # Fix specific patterns from the data
        text = re.sub(r"'T(\d+)'", r"''T\1''", text)  # T100, T20, etc.
        text = text.replace("kids\\'", "kids''")
        text = text.replace("\\'", "''")
        
        return text
    
    # Step 6: Extract and clean the VALUES section
    def extract_and_clean_values(content):
        """Extract the VALUES section and clean it"""
        
        # Find VALUES section
        values_match = re.search(r'VALUES\s+(.*?)(?:;|\n\s*--|\Z)', content, re.DOTALL | re.IGNORECASE)
        
        if values_match:
            values_content = values_match.group(1).strip()
            
            # Remove trailing semicolon and whitespace
            values_content = values_content.rstrip(';').strip()
            
            return values_content
        
        # If no VALUES found, assume the content is just the values
        return content.strip()
    
    # Apply all cleaning functions
    detected_table, columns = detect_table_and_columns(content)
    
    print(f"Detected table: {detected_table}")
    print(f"Detected columns: {len(columns)} columns")
    
    # Extract VALUES content
    values_content = extract_and_clean_values(content)
    
    # Apply cleaning steps
    cleaned_content = values_content
    cleaned_content = fix_newlines_and_formatting(cleaned_content)
    cleaned_content = fix_apostrophes(cleaned_content)
    cleaned_content = fix_business_terms(cleaned_content)
    cleaned_content = fix_quotes_and_special_chars(cleaned_content)
    
    # Generate column list for INSERT statement
    if columns:
        column_list = "(" + ", ".join(columns) + ")"
    else:
        # Default for CombinedAddons
        if "Addons" in detected_table:
            column_list = "(internal_id, id, additional_details, amount, amount_details, best_seller, billing_cycle, brand_name, description, is_new, limited_offer, processed_at, product_name, run_id, seasonal_offer, segment, url, vat_applicable, vat_included, vat_percentage, vat_percentage_details, validity, coverage_amount, coverage_amount_details, validity_details, minutes, minutes_type, data_allowance, data_allowance_details, data_type, minutes_details, coverage_types)"
        elif "Bio" in detected_table:
            column_list = "(internal_id, id, Biography, brand_name, processed_at, run_id, segment, url)"
        else:
            column_list = ""
    
    # Generate SQL Server script
    sql_server_content = f"""-- SQL Server INSERT Script for {detected_table}
-- Generated by Python cleaning script
-- Original MySQL data cleaned for SQL Server compatibility

-- Enable IDENTITY_INSERT
SET IDENTITY_INSERT {detected_table} ON;
SET NOCOUNT ON;

-- Begin Transaction for safety
BEGIN TRANSACTION;

TRY
    -- Insert statements
    INSERT INTO {detected_table} {column_list} VALUES
    {cleaned_content.rstrip(',')};
    
    -- Commit if successful
    COMMIT TRANSACTION;
    PRINT 'SUCCESS: Data inserted successfully into {detected_table}';
    
END TRY
BEGIN CATCH
    -- Rollback on error
    ROLLBACK TRANSACTION;
    PRINT 'ERROR: ' + ERROR_MESSAGE();
    PRINT 'Error occurred during insert into {detected_table}';
END CATCH

-- Disable IDENTITY_INSERT
SET IDENTITY_INSERT {detected_table} OFF;
SET NOCOUNT OFF;

-- Verify insertion
SELECT COUNT(*) as 'Total Records in {detected_table}' FROM {detected_table};
SELECT TOP 5 * FROM {detected_table} ORDER BY internal_id DESC;
"""
    
    # Save cleaned content
    try:
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(sql_server_content)
        
        print(f"✅ Cleaned data saved to: {output_file}")
        print(f"✅ Total size: {len(sql_server_content)} characters")
        
        # Count records
        record_count = len(re.findall(r'^\s*\(\d+,', cleaned_content, re.MULTILINE))
        print(f"✅ Total records: {record_count}")
        
        return True
        
    except Exception as e:
        print(f"Error writing file: {e}")
        return False

def create_sample_files():
    """Create sample input files for both tables"""
    
    # Sample CombinedAddons data
    addons_data = """(1, '67c7dc0a1f208d60dd031cc2-CCTV Video Analytics Add-On-4', 'Each analytic report add-on includes features like People Count, Heat Maps, and more.', 49, 'Per analytic report', 'No', 'Monthly', 'eand', 'Analytics add-on: Video Analytics, People Count, Heat Maps, Demographics', 'No', 'No', '2025-03-05 05:07:22', 'CCTV Video Analytics Add-On', 188, 'No', 'business', 'https://www.etisalat.ae/en/smb/products/digital-products/cctv/cctv-cloud-connectivity-4-cameras.html', 'True', 'False', 5, '5% VAT excluded', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(6, '67c7d1f81f208d60dd022e8d-Takaful Personal Accident Cover - AED 100,000-2', 'To subscribe, text \'T100\' to 1012.', 6, 'Monthly fee. VAT excluded.', 'No', 'Monthly', 'eand', 'Accidental life coverage at AED 5.5/month.', 'No', 'No', '2025-03-05 04:24:24', 'Takaful Personal Accident Cover - AED 100,000', 188, 'No', 'consumer', 'https://www.etisalat.ae/en/c/promotions/takaful-offer.html', 'True', 'False', 5, '5% VAT excluded.', '30 Days', 'AED 100,000', 'In the event of death by accident or Permanent Total Disablement (PTD) due to an accident.', 'The cover is valid for 30 days during the policy year for which contribution is paid.', NULL, NULL, NULL, NULL, NULL, NULL, NULL)"""
    
    # Sample CombinedBio data (from the user's document)
    bio_data = """(1, '67c7c0d01f208d60dd0098b0', 'Through our B2B portal, you can choose to pay fully or partially one or more bills by cheque. You can view and download your bill, generate a B2B reference number, and use it when depositing your cheque at any Etisalat Smart Service Machine. Access the B2B portal for easy bill management and payment.', 'eand', '2025-03-05 03:11:12', 188, 'business', 'https://www.etisalat.ae/en/smb/billing-and-payment/quick-and-easy-cheque-payment.html'),
(2, '67c7c0da1f208d60dd0099a3', 'Our mission at e& enterprise is to enable organizations to maximize their digital potential by being a partner in the digital shift through advanced technology and expert guidance. Our values focus on ambition, passion, people, and togetherness, driving us to exceed customer expectations and build collaborative ecosystems. Our strategy centers on digital transformation, with a focus on empowering businesses with cutting-edge technologies while prioritizing people. We aim to make a regional impact from Egypt to Oman in digital change. Our senior leadership guides us to success, spotting opportunities and keeping us on track. CEO Khalid Murshed emphasizes the extraordinary opportunities in the digital landscape driven by innovation. Our story began in 1976 as Etisalat, the UAE\\'s first telephone company, growing into the largest telco in the region. Today, as e& enterprise, we blend the strength of a telco with the agility of a managed service provider, helping businesses build a better tomorrow. In 2022, Etisalat transformed into e&, merging various business models into a global technology and investment conglomerate, diversifying into segments such as Etisalat UAE, e& international, e& life, e& capital, and e& enterprise previously known as Etisalat Digital.', 'eand', '2025-03-05 03:11:22', 188, 'e&_enterprise', 'https://www.eandenterprise.com/en/about-us.html'),
(3, '67c7c0dd1f208d60dd0099c0', 'Mobile & Roaming Services\\nConnecting mobile customers around the world\\nWith more than 780 mobile partners, our mobility services offer a multilateral roaming service and enable a one-stop shop to mobile customers around the world. Signalling services & VAS\\nWe are a one-stop shop for mobile operators, enabling 2/3G roaming services with global reach for more than 700 mobile operators along with value-added services. Our SS7 Signalling service is essential for establishing highest quality international roaming and with ANSI signalling service we enable roaming of the GSM mobile operators in the North American region.\\nRoaming Steering Tool\\nA reliable steering tool, Proactively steering the roaming traffic to the preferred network, based on quality of service and commercial\\nSignalling Analytics\\nFull reporting and notification systems, Generating reports and alarms-notification to guarantee the QoS and identify the silent roamers in real-time base; presenting different KPIs\\nSS7 Signalling Firewall\\nA great support system, Recognised as one of the best telecom innovative solution\\nOur Services\\nAntispam Policy\\nClick here to download the SMS service Antispam Policy\\nDownload PDF\\nContact Sales\\nShare your details and our sales team will get back to you.\\nRequest A Call Back\\n© 2025 e&. All Rights Reserved.\\nCareers Terms & Conditions Privacy Policy Code of Conduct', 'eand', '2025-03-05 03:11:25', 188, 'carrier_and_wholesale', 'https://www.eand.com/en/whoweare/carrier-and-wholesale/services/mobile-and-roaming.html')"""
    
    with open('mysql_addons_data.txt', 'w', encoding='utf-8') as file:
        file.write(addons_data)
    
    with open('mysql_bio_data.txt', 'w', encoding='utf-8') as file:
        file.write(bio_data)
    
    print("✅ Sample input files created:")
    print("   - mysql_addons_data.txt (for CombinedAddons)")
    print("   - mysql_bio_data.txt (for CombinedBio)")

def main():
    """Main function to run the cleaner"""
    
    print("🔧 MySQL to SQL Server Data Cleaner v2.0")
    print("Supports: CombinedAddons, CombinedBio, and other tables")
    print("=" * 60)
    
    # Get user input for file and table
    print("\nAvailable input files:")
    files = [f for f in os.listdir('.') if f.endswith('.txt') or f.endswith('.sql')]
    
    if not files:
        print("⚠️  No input files found.")
        create_sample = input("Would you like to create sample input files? (y/n): ")
        
        if create_sample.lower() == 'y':
            create_sample_files()
            print(f"\n📝 Please add your MySQL INSERT data to the appropriate file and run this script again.")
            return
        else:
            print("Please create a .txt file with your MySQL INSERT data.")
            return
    
    print("Found files:")
    for i, file in enumerate(files, 1):
        print(f"  {i}. {file}")
    
    try:
        choice = int(input(f"\nSelect file (1-{len(files)}): ")) - 1
        input_file = files[choice]
    except (ValueError, IndexError):
        print("Invalid choice. Using default...")
        input_file = files[0]
    
    # Generate output filename
    base_name = os.path.splitext(input_file)[0]
    output_file = f"{base_name}_cleaned_sqlserver.sql"
    
    # Clean the data
    success = clean_sql_data(input_file, output_file)
    
    if success:
        print("\n🎉 Data cleaning completed successfully!")
        print(f"📄 Cleaned SQL Server script saved as: {output_file}")
        print("\n📋 Next steps:")
        print("1. Review the generated SQL file")
        print("2. Run it in SQL Server Management Studio")
        print("3. Check for any remaining syntax errors")
        print("4. Verify data integrity after insertion")
    else:
        print("\n❌ Data cleaning failed. Please check the error messages above.")

if __name__ == "__main__":
    main()