import pandas as pad 
import pyodbc 
import openpyxl 
import re 
import os 
import numpy as np 
import json 
import shutil 
#import html2text 
from datetime import datetime 
import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
import snowflake.connector 
import logging 
from sys import path 
import pandas as pd 
path.append('\\Windows\Microsoft.NET\\assembly\\GAC_MSIL\\Microsoft.AnalysisServices.AdomdClient\\v4.0_13.0.0.0__89845dcd8080cc91\\') 
from pyadomd import Pyadomd 
 

log_file_path = 'C:\JACKIN.gar.corp.intel.com\Retail\errors\error_log.txt' 

if os.path.exists(log_file_path): 
    os.remove(log_file_path) 
logging.basicConfig(filename=log_file_path, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s') 
 

def execute_sql_query(query, connection, db): 
    try: 
        if db.lower().strip() == 'snowflake': 
            conn = snowflake.connector.connect( 
                user=connection['snowflake_user'], 
                password=connection['snowflake_password'], 
                account=connection['snowflake_account'], 
                warehouse=connection['snowflake_warehouse'], 
                database=connection['snowflake_database'], 
                schema=connection['snowflake_schema'], 
                role=connection['snowflake_role'] 
            ) 
            with conn.cursor() as cursor: 
                cursor.execute(query) 
                df = pd.DataFrame(cursor.fetchall(), columns=[x[0] for x in cursor.description]) 
        elif db.lower().strip() == 'cube': 
            with Pyadomd(connection) as cube_conn: 
                df = pd.read_sql(query, cube_conn) 
        else: 
            with pyodbc.connect(**connection, autocommit=True) as conn: 
                df = pd.read_sql(query, conn) 
        return df 
    except Exception as e: 
        print(f"Error executing SQL query: {str(e)}") 
        error_message = f"Error executing SQL query: {str(e)}" 
        logging.error(error_message) 
        return None 

def compare_dataframes(df1, df2, merge_keys, diff_columns): 
    try: 
        df1 = df1.rename(columns=lambda x: x.replace('[', '').replace(']', '')) 
        df2 = df2.rename(columns=lambda x: x.replace('[', '').replace(']', '')) 
        df1_sorted = df1.sort_index() 
        df2_sorted = df2.sort_index() 
        df1.columns = map(str.lower, df1.columns) 
        df1_sorted.columns = map(str.lower, df1_sorted.columns) 
        df2_sorted.columns = map(str.lower, df2_sorted.columns) 
        df1_merge_keys = df1_sorted[merge_keys].columns.tolist() 
        df2_merge_keys = df2_sorted[merge_keys].columns.tolist() 
        for key in df1_merge_keys: 
            df1_sorted[key].fillna('BLANK_MERGE_KEY', inplace=True) 
        for key in df2_merge_keys: 
            df2_sorted[key].fillna('BLANK_MERGE_KEY', inplace=True) 
        df1_sorted, df2_sorted = df1_sorted.align(df2_sorted, axis=1) 
        final_df = df1.merge(df2_sorted, left_on=df1_merge_keys, right_on=df2_merge_keys, how='outer', suffixes=['_source', '_target']) 
        for key in df1_merge_keys: 
            final_df[key].replace('BLANK_MERGE_KEY', np.nan, inplace=True) 
        for col in diff_columns: 
            source_col = f'{col}_source' 
            target_col = f'{col}_target' 
            if source_col in final_df.columns and target_col in final_df.columns: 
                final_df[f'Diff_{col}'] = final_df[source_col] - final_df[target_col] 
        final_df = final_df[final_df[[col for col in final_df.columns if col.startswith('Diff_')]].any(axis=1)] 
        return final_df 
    except Exception as e: 
        print(f"Error in comparing dataframes: {str(e)}") 
        error_message = f"Error in comparing dataframes: {str(e)}" 
        logging.error(error_message) 
        return None 

def execute_queries_from_file(file_path, connection, db): 
    try: 
        with open(file_path, "r") as query_file: 
            queries = query_file.read().split(';') 
        df_list = [] 
        for query in queries: 
            if query.strip(): 
                df = execute_sql_query(query.strip(), connection, db) 
                df_list.append(df) 
        return df_list 
    except Exception as e: 
        print(f"Error in execute queries from file: {str(e)}") 
        error_message = f"Error in execute queries from file: {str(e)}" 
        logging.error(error_message) 
        return None 

def read_excel_path(file_path): 
    try: 
        df = pd.read_excel(file_path) 
        return df['Path'].values[0] 
    except Exception as e: 
        print(f"Error in reading excel path: {str(e)}") 
        error_message = f"Error in reading excel path: {str(e)}" 
        logging.error(error_message) 
        return None 

def get_merge_keys(df): 
    try: 
        merge_keys = df.loc[df['Unique_Key'] == 'Yes', 'Column'].str.lower().tolist() 
        return merge_keys 
    except Exception as e: 
        print(f"Error in getting merge keys: {str(e)}") 
        error_message = error_message = f"Error in reading excel path: {str(e)}" 
        logging.error(error_message) 
        return None 

def get_diff_columns(df): 
    try: 
        diff_columns = df.loc[df['Unique_Key'] == 'eval', 'Column'].str.lower().tolist() 
        return diff_columns 
    except Exception as e: 
        print(f"Error in getting difference columns: {str(e)}") 
        error_message = error_message = f"Error in getting difference columns: {str(e)}" 
        logging.error(error_message) 

        return None 

def get_source_target_files(df): 
    try: 
        source_file = df['Source'].values[0] 
        target_file = df['Target'].values[0] 
        return source_file, target_file 
    except Exception as e: 
        print(f"Error in getting source and target files: {str(e)}") 
        error_message = f"Error in getting source and target files: {str(e)}" 
        logging.error(error_message) 
        return None 

def cube_to_cube_comparison(source_connection_str, target_connection_str, source_query, target_query): 
    try: 
        with Pyadomd.connect(source_connection_str) as source_conn: 
            df_source = pd.read_sql(source_query, source_conn) 
        with Pyadomd.connect(target_connection_str) as target_conn: 
            df_target = pd.read_sql(target_query, target_conn) 
        return df_source, df_target 
    except Exception as e: 
        print(f"Error in cube to cube comparison: {str(e)}") 
        error_message = f"Error in cube to cube comparison: {str(e)}" 
        logging.error(error_message) 
        return None 

def snowflake_to_cube_comparison(snowflake_connection_details, cube_connection_str, snowflake_query, cube_query): 
    try: 
        conn = snowflake.connector.connect( 
            user=snowflake_connection_details['snowflake_user'], 
            password=snowflake_connection_details['snowflake_password'], 
            account=snowflake_connection_details['snowflake_account'], 
            warehouse=snowflake_connection_details['snowflake_warehouse'], 
            database=snowflake_connection_details['snowflake_database'], 
            schema=snowflake_connection_details['snowflake_schema'], 
            role=snowflake_connection_details['snowflake_role'] 
        ) 

        with conn.cursor() as cursor: 
            cursor.execute(snowflake_query) 
            df_source = pd.DataFrame(cursor.fetchall(), columns=[x[0] for x in cursor.description]) 
        with Pyadomd.connect(cube_connection_str) as cube_conn: 
            df_target = pd.read_sql(cube_query, cube_conn) 
        return df_source, df_target 
    except Exception as e: 
        print(f"Error in snowflake to cube comparison: {str(e)}") 
        error_message = f"Error in snowflake to cube comparison: {str(e)}" 
        logging.error(error_message) 
        return None 
 
def load_data_from_excel(excel_path, source_connection, target_connection, source_db, target_db): 
    try: 
        df = pd.read_excel(excel_path) 
        if (source_db.lower()).strip() == 'cube': 
            source_conn_details = source_connection 
        else: 
            source_conn_details = json.loads(source_connection) 
        if (target_db.lower()).strip() == 'cube': 
            target_conn_details = target_connection 
        else: 
            target_conn_details = json.loads(target_connection) 
        df_source = execute_queries_from_file(df['Source'].values[0], source_conn_details, source_db)[0] 
        df_target = execute_queries_from_file(df['Target'].values[0], target_conn_details, target_db)[0] 
        return df_source, df_target 
    except Exception as e: 
        print(f"Error in loading data from excel: {str(e)}") 
        error_message = f"Error in loading data from excel: {str(e)}" 
        logging.error(error_message) 
        return None 

def process_query_file(df_source, df_target, merge_keys, diff_columns): 
    try: 
        df_diff = compare_dataframes(df_source, df_target, merge_keys, diff_columns) 
        return df_diff 
    except Exception as e: 
        print(f"Error processing query file: {str(e)}") 
        error_message = f"Error processing query file: {str(e)}" 
        logging.error(error_message) 
        return None 

def save_output_to_excel(output_file, dataframes, source_file, target_file, test_case_folder): 
    try: 
        output_folder = os.path.join(test_case_folder, "output") 
        os.makedirs(output_folder, exist_ok=True) 
        output_file_path = os.path.join(output_folder, "df_diff.xlsx") 
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as excel_writer: 
            for i, styled_diff in enumerate(dataframes, start=1): 
                sheet_name = f"{os.path.basename(source_file)} - {os.path.basename(target_file)}" 
                if len(dataframes) > 1: 
                    sheet_name += f" {i}" 
                try: 
                    styled_diff.to_excel(excel_writer, sheet_name=sheet_name) 
                except ValueError: 
                    print(f"No differences found in {sheet_name}. Skipping sheet.") 
        print(f"Comparison and row count data saved to {output_file_path}.") 
    except Exception as e: 
        print(f"Error in saving output to excel: {str(e)}") 
        error_message = f"Error in saving output to excel: {str(e)}" 
        logging.error(error_message) 
        return None 

def create_processed_folder(output_file, source_file, target_file, test_case_folder): 
    try: 
        processed_folder = os.path.join(test_case_folder, "processed") 
        os.makedirs(processed_folder, exist_ok=True) 
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S") 
        processed_output_file = os.path.join(processed_folder, f"df_diff_{timestamp}.xlsx") 
        shutil.copy(output_file, processed_output_file) 
        shutil.copy(source_file, os.path.join(processed_folder, f"source_test_{timestamp}.txt")) 
        shutil.copy(target_file, os.path.join(processed_folder, f"target_test_{timestamp}.txt")) 
        print("Query and output files copied to the 'processed' folder with timestamps.") 
    except Exception as e: 
        print(f"Error in creating processed folder: {str(e)}") 
        error_message = f"Error in creating processed folder: {str(e)}" 
        logging.error(error_message) 
        return None 

def send_email(test_results, sender_email, sender_password, receiver_email, server_address, output_results_folder, Environment,log_file_path): 
    try: 
        smtp_server = 'smtpauth.intel.com' 
        smtp_port = 587 
        sender_email = sender_email 
        sender_password = sender_password 
        receiver_email = receiver_email 
        total_test_cases = len(test_results) 
        passed_count = sum(result['status'] == 'Pass' for result in test_results.values()) 
        failed_count = sum(result['status'] == 'Fail' for result in test_results.values()) 
        error_count = sum(result['status'] == 'Error' for result in test_results.values()) 
        test_case_summary_table = '<table><tr><th colspan="4">Test Case Summary</th></tr><tr><th>Test Case #</th><th>Test Case Description</th><th>Result</th><th>Test Case Execution Time</th></tr>' 
        for result in test_results.values(): 
            test_case_id = result['test_case_id'] 
            test_case_description = result['test_case_description'] 
            test_case_folder_name = result['test_case_folder_name'] 
            email_subject = f"Automated_Test_Results_{Environment} - {test_case_folder_name}" 
            test_case_execution_time = result['execution_time'] 
            if result['status'] == 'Fail': 
                output_folder_link = f'<a href="{test_case_id}/output">Output Folder</a>' 
                result_label = f'<a href=file:///{output_results_folder}\\{test_case_id}/output/df_diff.xlsx><span style="color: red;">[FAILED]</span></a>' 
                result = f'<span style="color: red;">[FAILED]</span>' 
            elif result['status'] == 'Pass': 
                result = '[PASS]' 
                result_label = f'<span style="color: green;">[PASS]</span>' 
            else: 
                result = '[ERROR]' 
                result_label = f'<span style="color: red;">[ERROR]</span>' 
            test_case_summary_table += f'<tr><td><a href=file:///{output_results_folder}\\{test_case_id}\\>{test_case_id}</a></td><td>{test_case_description}</td><td>{result_label}</td><td>{test_case_execution_time}</td></tr>' 
        test_case_summary_table += '</table>' 

        html_content = f''' 
        <html> 
        <head> 
            <style> 
                table {{ 
                    border-collapse: collapse; 
                    border: 1px solid black;  
                    border-radius: 8px; 
                    width: 100%; 
                }} 
                th, td {{ 
                    border: 0px solid black;   
                    padding: 8px; 
                    text-align: left; 
                }} 
                th {{ 
                    background-color: #0071c5; 
                    color: white; 
                }} 
                .summary-table {{ 
                    width: 40%; 
                }} 

                h2 {{ 

                    font-size: 18px;   

                }} 

            </style> 

        </head> 

        <body> 

            <h2>Test Result Summary</h2> 

            <table class="summary-table"> 

                <tr> 

                    <th>Total Test Cases</th> 

                    <th>Passed</th> 

                    <th>Failed</th> 

                    <th>Error</th> 

                </tr> 

                <tr> 

                    <td>{total_test_cases}</td> 

                    <td>{passed_count}</td> 

                    <td>{failed_count}</td> 

                    <td>{error_count}</td> 

                </tr> 

            </table> 

            <h2>Test Case Summary</h2> 

            {test_case_summary_table} 

        </body> 

        </html> 

        ''' 

  

        message = MIMEMultipart() 

        message['From'] = sender_email 

        message['To'] = ", ".join(receiver_email) 

        message['Subject'] = email_subject 

        html_attachment = MIMEText(html_content, 'html') 

        message.attach(html_attachment) 

  

        with open(log_file_path, 'r') as log_file: 

            log_attachment = MIMEText(log_file.read()) 

            log_attachment.add_header('Content-Disposition',f'attachment; filename="{os.path.basename(log_file_path)}"') 

            message.attach(log_attachment) 

  

        try: 

  

            with smtplib.SMTP(smtp_server, smtp_port) as server: 

                server.starttls() 

                server.login(sender_email, sender_password) 

                server.sendmail(sender_email, receiver_email, message.as_string()) 

            print('Email sent successfully!') 

        except Exception as e: 

            print(f'Failed to send email. Error: {str(e)}') 

            error_message = f'Failed to send email. Error: {str(e)}' 

            logging.error(error_message) 

    except Exception as e: 

        print(f"Error in send email: {str(e)}") 

        error_message = f"Error in send email: {str(e)}" 

        logging.error(error_message) 

        return None 

  

def process_excel_queries(output_results_folder, excel_path, source_connection, target_connection, test_case_id, source_db, target_db): 

    try: 

  

        path = read_excel_path(excel_path) 

        df = pd.read_excel(excel_path) 

        merge_keys = get_merge_keys(df) 

        diff_columns = get_diff_columns(df) 

        source_file, target_file = get_source_target_files(df) 

        source_file_path = os.path.join(path, source_file) 

        target_file_path = os.path.join(path, target_file) 

  

        start_time = datetime.now() 

        try: 

  

            df_source, df_target = load_data_from_excel(excel_path, source_connection, target_connection, source_db, target_db) 

        except Exception as e: 

            print(f"Error loading data from excel: {str(e)}") 

            error_message = f"Error loading data from excel: {str(e)}" 

            logging.error(error_message) 

            return 'Error' 

        end_time = datetime.now() 

        execution_time = end_time - start_time 

  

        try: 

            df_diff = process_query_file(df_source, df_target, merge_keys, diff_columns) 

        except Exception as e: 

            print(f"Error processing query file: {str(e)}") 

            error_message = f"Error processing query file: {str(e)}" 

            logging.error(error_message) 

            return 'Error' 

  

        test_case_folder_name = os.path.splitext(os.path.basename(excel_path))[0] 

        test_case_folder = output_results_folder + test_case_id 

        output_file = os.path.join(test_case_folder, "output", "df_diff.xlsx") 

        test_case_execution_time = str(execution_time) 

  

        save_output_to_excel(output_file, [df_diff], source_file_path, target_file_path, test_case_folder) 

        create_processed_folder(output_file, source_file_path, target_file_path, test_case_folder) 

  

        test_case_status = 'Pass' if df_diff.empty else 'Fail' 

  

        df.loc[0, 'Status'] = test_case_status 

        df.to_excel(excel_path, index=False) 

        return test_case_status 

    except Exception as e: 

        print(f"Error in processing excel queries: {str(e)}") 

        error_message = f"Error in processing excel queries: {str(e)}" 

        logging.error(error_message) 

        return 'Error' 

  

def main(master_folder, sender_email, sender_password, receiver_email, server_address, output_results_folder, Environment): 

    try: 

  

        excel_files = [] 

        master_folder = master_folder 

  

        for filename in os.listdir(master_folder): 

            if filename.endswith('.xlsx'): 

                file_path = os.path.join(master_folder, filename) 

                df_master = pd.read_excel(file_path) 

                excel_files.append(df_master) 

  

        for df_master in excel_files: 

            test_results = {} 

            test_case_id_list = [] 

            test_case_description_list = [] 

  

            try: 

  

                for _, row in df_master.iterrows(): 

                    excel_file_path = row['TestCasePath'] 

                    path_parts = excel_file_path.split("/") 

                    test_case_folder_name = path_parts[4] 

                    test_case_id = row['TestCaseID'] 

                    source_connection = row['Source Connection'] 

                    target_connection = row['Target Connection'] 

                    source_db = row['Source DB'] 

                    target_db = row['Target DB'] 

  

                    print(f"Processing test case: {excel_file_path}") 

                    start_time = datetime.now() 

  

                    test_case_status = process_excel_queries(output_results_folder, excel_file_path, source_connection, target_connection, test_case_id, source_db, target_db) 

  

                    end_time = datetime.now() 

                    execution_time = end_time - start_time 

                    print(f"Test case status: {test_case_status}") 

  

                    test_case_description = df_master.loc[df_master['TestCaseID'] == test_case_id, 'TestCaseDescription'].values[0] 

                    test_results[excel_file_path] = { 

                        'status': test_case_status, 

                        'execution_time': execution_time, 

                        'test_case_id': test_case_id, 

                        'test_case_description': test_case_description, 

                        'test_case_folder_name': test_case_folder_name 

                    } 

                    test_case_id_list.append(test_case_id) 

                    test_case_description_list.append(test_case_description) 

            except Exception as e: 

                print(f"Error processing test case: {excel_file_path}. Error: {str(e)}") 

                error_message = f"Error processing test case: {excel_file_path}. Error: {str(e)}" 

                logging.error(error_message) 

                test_results[excel_file_path] = { 

                    'status': 'Error', 

                    'execution_time': None, 

                    'test_case_id': test_case_id, 

                    'test_case_description': test_case_description, 

                    'test_case_folder_name': test_case_folder_name 

                } 

  

            send_email(test_results, sender_email, sender_password, receiver_email, server_address, output_results_folder, Environment,log_file_path) 

  

    except Exception as e: 

        print(f"Error in main function: {str(e)}") 

        error_message = f"Error in main function: {str(e)}" 

        logging.error(error_message) 

        return None 

  

  

if __name__ == "__main__": 

    server_address = '\\\JACKIN.gar.corp.intel.com' 

    server_address_results = '\\JACKIN.gar.corp.intel.com' 

    master_folder = server_address+'\Automation\TestScripts\Retail_CNG_WW_Cube\Master_dryrun' 

    sender_email = 'sys_rpssnap@intel.com' 

    sender_password = 'RetailCubesnapshot@1234' 

    receiver_email = ["ranganath.kumarx.kalivili.munaswamy@intel.com","shanthiprasadx.jain.t.s@intel.com"]#ranganath.kumarx.kalivili.munaswamy@intel.com'#'#'retail.cloud.apt@intel.com' 

    Environment = 'QA' 

    output_results_folder = server_address_results + '\\Retail\\Results_ww_qa\\' 

    try: 

        main(master_folder, sender_email, sender_password, receiver_email, server_address, output_results_folder, Environment) 

    except Exception as e: 

        print(f"Error in main execution: {str(e)}") 