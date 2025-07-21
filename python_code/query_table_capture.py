import csv
import re
import sys
import os

def extract_tables(query):
    # CHANGE 1: Updated to handle both legacy and Gridiron patterns
    legacy_pattern = r'AwsDataCatalog\.[^.\s]+\.[^.\s]+_vw'  # Legacy: AwsDataCatalog.schema.table_vw
    gridiron_pattern = r'[a-zA-Z_]+_ptc\.[a-zA-Z_0-9]+'     # Gridiron: schema_ptc.table_name
    
    legacy_tables = re.findall(legacy_pattern, query, re.IGNORECASE)
    gridiron_tables = re.findall(gridiron_pattern, query, re.IGNORECASE)
    
    # CHANGE 2: Combine both types and preserve order
    all_tables = []
    
    # Add legacy tables with their full names
    for table in legacy_tables:
        all_tables.append(('legacy', table))
    
    # Add gridiron tables with their full names  
    for table in gridiron_tables:
        all_tables.append(('gridiron', table))
    
    # Remove duplicates while preserving order
    seen = set()
    unique_tables = []
    for env_type, table in all_tables:
        if table not in seen:
            unique_tables.append((env_type, table))
            seen.add(table)
    
    print(f"Tables found: {unique_tables}")
    return unique_tables

def is_query_complete(table_info_list):
    # CHANGE 3: Updated to work with new tuple structure (environment, table)
    return 1 if table_info_list else 0

def get_simplified_table_name(table, environment):
    # CHANGE 4: Enhanced to handle both environments differently
    if environment == 'legacy':
        # Legacy: Remove AwsDataCatalog prefix and _vw suffix
        table_name = table.split('.')[-1]  # Get the last part after dots
        table_name = table_name.replace('_vw', '')  # Remove _vw suffix
        # Remove club code (last part after underscore)
        parts = table_name.split('_')
        if len(parts) > 1:
            table_name = '_'.join(parts[:-1])
        return table_name
    else:  # gridiron
        # Gridiron: Extract table name after schema (remove schema_ptc. prefix)
        # Example: fulfillment_ptc.ptc_ticketing_single_game_propensity_score_legacy -> ticketing_single_game_propensity_score_legacy
        table_name = table.split('.')[-1]  # Get part after the dot
        if table_name.startswith('ptc_'):
            table_name = table_name[4:]  # Remove 'ptc_' prefix
        return table_name

def determine_primary_environment(table_info_list, completeness):
    # CHANGE 5: Updated to handle N/A case when completeness = 0
    if completeness == 0:
        return 'N/A'
    
    if not table_info_list:
        return 'unknown'
    
    env_counts = {'legacy': 0, 'gridiron': 0}
    for env_type, _ in table_info_list:
        env_counts[env_type] += 1
    
    # Return the environment with more tables, or 'mixed' if equal
    if env_counts['legacy'] > env_counts['gridiron']:
        return 'legacy'
    elif env_counts['gridiron'] > env_counts['legacy']:
        return 'gridiron'
    else:
        return 'mixed' if env_counts['legacy'] > 0 and env_counts['gridiron'] > 0 else 'legacy'

def process_file(input_file, output_file):
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        
        # CHANGE 6: Updated fieldnames with underscore convention and extended to 6 tables
        fieldnames = ['timestamp_et', 'user', 'club', 'query', 'table_used', 'completeness', 
                      'first_table_used', 'second_table_used', 'third_table_used', 
                      'fourth_table_used', 'fifth_table_used', 'sixth_table_used',
                      'table_used_num', 'data_environment']
        
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            query = row.get('Query', '')
            table_info_list = extract_tables(query)  # Now returns [(env, table), ...]
            completeness = is_query_complete(table_info_list)
            
            # CHANGE 7: Extract just table names for simplified display
            simplified_tables = [get_simplified_table_name(table, env) for env, table in table_info_list]
            
            # CHANGE 8: Count unique tables used
            table_used_num = len(simplified_tables)
            
            # CHANGE 9: Determine primary data environment (now handles completeness = 0)
            data_environment = determine_primary_environment(table_info_list, completeness)
            
            new_row = {
                'timestamp_et': row.get('Timestamp (ET)', ''),
                'user': row.get('User', ''),
                'club': row.get('Club', ''),
                'query': query,
                'table_used': ','.join(simplified_tables),
                'completeness': completeness,
                'first_table_used': simplified_tables[0] if len(simplified_tables) > 0 else '',
                'second_table_used': simplified_tables[1] if len(simplified_tables) > 1 else '',
                'third_table_used': simplified_tables[2] if len(simplified_tables) > 2 else '',
                'fourth_table_used': simplified_tables[3] if len(simplified_tables) > 3 else '',
                'fifth_table_used': simplified_tables[4] if len(simplified_tables) > 4 else '',
                'sixth_table_used': simplified_tables[5] if len(simplified_tables) > 5 else '',
                'table_used_num': table_used_num,
                'data_environment': data_environment
            }
            
            writer.writerow(new_row)

    print(f"Processing complete. Output written to {output_file}")

if __name__ == "__main__":
    # Set paths relative to script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    raw_data_dir = os.path.join(project_root, "raw_data")
    output_dir = os.path.join(project_root, "curated_output")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    if len(sys.argv) != 2:
        print("Usage: python query_table_capture.py <filename>")
        print("Example: python query_table_capture.py data.csv")
        sys.exit(1)
    
    filename = sys.argv[1]
    if not filename.endswith('.csv'):
        filename += '.csv'
    
    input_file = os.path.join(raw_data_dir, filename)
    output_file = os.path.join(output_dir, "query_table_captured.csv")
    
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        sys.exit(1)
    
    process_file(input_file, output_file)