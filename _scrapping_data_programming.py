import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
import csv
import time

from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.chrome.service import Service as ChromeService 
from webdriver_manager.chrome import ChromeDriverManager 

import shutil

# instantiate options 
options = webdriver.ChromeOptions() 
# run browser in headless mode 
options.headless = True 
# load website 
url = "https://www.set.or.th/en/market/statistics/program-trading-value"

# Define column names for the DataFrames
columns_df1 = [
    'Program Trading Buy',
    'Program Trading Sell',
    'Program Trading Net',
    'Non Program Trading Buy',
    'Non Program Trading Sell',
    'Non Program Trading Net',
    'Total Buy',
    'Total Sell']

items_to_delete = ['Symbol', 'Market', 'Program', 'Trading', 'Value', '(Baht)', '%', 'Program', 
                   'Trading', 'Value', 'Comparing', 'with', 'Auto', 'Matching', 'SET', 'mai']

def get_lsited_number(text):
    words = text.split()
    numbers = [float(word.replace(',', '')) 
                    for word in words 
                            if (word.replace(',', '').replace('.', '').replace('-', '').isdigit() 
                                or 
                            word.replace(',', '').replace('.', '').replace('-', '').lstrip('+-').isdigit())]
    return numbers

def get_text_only(text):
    words = text.split()
    # text_only = [word for word in words if not any(char.isdigit() for char in word)]
    text_only = [word for word in words if any(char.isalpha() for char in word)]
    return text_only

def update_df(main_df, new_df):
    for col in new_df.columns:
        if col == 'datetime':
            continue
        # print('col:',col, new_df[col].values[0])
        if col in main_df.columns:
            main_df.loc[new_df['datetime'], col] = new_df[col].values[0]
        else:
            # print('new col')
            main_df.loc[new_df['datetime'], col] = new_df[col].values[0]
    main_df = main_df.fillna(0)
    return main_df

def get_numbers_and_text_from_driver(df1, df2, print_value=False, formatted_date=datetime.now()):
    # select elements by class name 
    elements = driver.find_elements(By.CLASS_NAME, 'table-responsive')
    
    for idx, element in enumerate(elements):
        if idx == 1:
            values = get_lsited_number(element.text)
            if print_value:
                print('\t', len(values), values)
            if len(values)==8:
                df_temp = pd.DataFrame([values], columns=columns_df1)
                df_temp['datetime'] = [formatted_date]
                for col in df1.columns:
                    df1.loc[df_temp['datetime'], col] = df_temp[col].values[0]
        elif idx == 3:
            today_symbols = get_text_only(element.text)
            # print('\t', today_symbols)
            today_symbols = [item for item in today_symbols if item not in items_to_delete]
            values = get_lsited_number(element.text)[1::2]
            if print_value:
                print('\t', len(today_symbols), today_symbols)
                print('\t', len(values), values)
            if len(values)>=1:
                df_temp = pd.DataFrame([values], columns=today_symbols)
                df_temp['datetime']=[formatted_date]
                print(len(df_temp.columns))
                df2 = update_df(df2, df_temp)

    return df1, df2


def csv_to_js(csv_file, js_file):
    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Get the header (column names)

        data = {
            'labels': [],
        }
        for i, col_name in enumerate(header):
            if i == 0:
                continue  # Skip the datetime column
            data[col_name] = []

        for row in reader:
            data['labels'].append(row[0])  # Assuming the datetime is in the first column
            for i, value in enumerate(row[1:]):  # Start from the second column
                col_name = header[i+1]  # Adjust index to account for skipping datetime
                # print('value=', value)
                data[col_name].append(float(value))

    with open(js_file, 'w') as jsfile:
        jsfile.write('var my_data = {\n')
        for key, values in data.items():
            if key == 'labels':
                jsfile.write(f"    '{key}': [{', '.join(map(repr, values))}],\n")
            else:
                jsfile.write(f"    '{key}': [{', '.join(map(str, values))}],\n")
        jsfile.write('};')

    print(f"CSV file '{csv_file}' converted to JavaScript file '{js_file}'.")


def from_last_date(file_path):
    # Get today's date
    today_date = (datetime.now()-timedelta(days=1))
    start_date = today_date
    print(today_date)
    today_date = today_date.strftime("%A, %B %d, %Y").replace(" 0", " ")
    
    # Read the DataFrame and get the last date
    read_df = pd.read_csv(file_path)
    last_date = read_df['datetime'][0]
    
    # Convert last date string to datetime object
    date1 = datetime.strptime(last_date, "%A, %B %d, %Y")
    
    # Convert today's date string to datetime object
    date2 = datetime.strptime(today_date, "%A, %B %d, %Y")
    
    # Calculate the difference in days
    num_days = abs((date2 - date1).days)
    print('num_days: ',num_days)
    formatted_dates = []
    
    # Generate formatted dates for each day
    for i in range(num_days):
        current_date = start_date - timedelta(days=i)
        formatted_date = current_date.strftime("%A, %B %d, %Y")
        formatted_date = formatted_date.replace(" 0", " ")  # Remove leading zero for single digit days
        formatted_dates.append(formatted_date)
    
    return formatted_dates

if __name__ == '__main__':
    # last update or normal (from scratch)
    cond = 'last'
    
    df1 = pd.DataFrame(columns=columns_df1)
    df2 = pd.DataFrame(columns=[])
    
    if cond=='normal':
        start_date = datetime.now() - timedelta(days=1)
        num_days = 120
        formatted_dates = []
        for i in range(num_days):
            current_date = start_date - timedelta(days=i)
            formatted_date = current_date.strftime("%A, %B %d, %Y")
            formatted_date = formatted_date.replace(" 0", " ")  # Remove leading zero for single digit days
            formatted_dates.append(formatted_date)
    elif cond=='last':
        # Example usage
        file_path = './DataMining/rpa/df_securities.csv'
        df_old = pd.read_csv(file_path)
        formatted_dates = from_last_date(file_path)
    
    print(formatted_dates)
    if len(formatted_dates)==0:
        print('data is up to date.')
    else:
        print(formatted_dates[0], ' to ',formatted_dates[-1])
    
        
        df1['datetime'] = formatted_dates
        df1.set_index('datetime', inplace=True)
        df2['datetime'] = formatted_dates
        df2.set_index('datetime', inplace=True)

        # instantiate options 
        options = webdriver.ChromeOptions() 
        # run browser in headless mode 
        options.headless = True 
        # instantiate driver 
        driver = webdriver.Chrome(service=ChromeService( 
            ChromeDriverManager().install()), options=options) 
        driver.get(url)


        print('START')
        skip_origin = False
        # Now iterate over the list of formatted dates and perform Selenium actions
        for formatted_date in tqdm(formatted_dates):
            if datetime.strptime(formatted_date, "%A, %B %d, %Y").weekday() >= 5:  # Saturday (5) or Sunday (6)
                print(f">>>> SKIP WEEKEND: {formatted_date}")
                continue  # Skip to the next iteration of the loop
            
            print('DATE=', formatted_date)
            origin = driver.find_element(By.XPATH, "//input[@placeholder='Select date']")
            if not skip_origin:
                origin.click()
                time.sleep(1)
            skip_origin = False
            try:
                day_element_xpath = f"//span[@aria-label='{formatted_date}']"
                day_element = driver.find_element(By.XPATH, day_element_xpath)
                day_element.click()
                time.sleep(1) 
                df1, df2 = get_numbers_and_text_from_driver(df1, df2, True, formatted_date=formatted_date)
            except Exception as e:
                print("An error occurred:", e)
                print(f'>>>>>> ERROR: {formatted_date}.')
                break
            
            # Check if the date part includes " 1,"
            if " 1," in formatted_date:
                # 1st date >>> CHANGE MONTH"
                origin = driver.find_element(By.XPATH,"//input[@placeholder='Select date']")
                origin.click()
                time.sleep(1)
                month_backward = driver.find_element(By.XPATH, "//div[@class='vc-arrow is-left']//*[name()='svg']")
                month_backward.click()
                skip_origin = True
                # BACKWARD COMPLETE
                time.sleep(1)
                
        print('saving df1 and df2')
        df1_cleaned = df1[~(df1==0.0).all(axis=1)]
        df2_cleaned = df2[~(df2==0.0).all(axis=1)]
        
        print('df2_cleaned')
        print(df2_cleaned.columns, df2_cleaned.head())
        
        # df1_cleaned.to_csv('./DataMining/rpa/df_summary.csv')
        if cond=='normal':
            df2_cleaned.to_csv('./DataMining/rpa/df_securities.csv')
        
        if cond=='last':
            print('df2_cleaned')
            print(df2_cleaned.index, df2_cleaned.columns)
            print('df_old')
            df_old = df_old.set_index('datetime')
            print(df_old.index, df_old.columns)
            
            result_final = pd.concat([df2_cleaned, df_old], axis=0).fillna(0.0)
            print('result_final: ')
            print(result_final.head())
            print(result_final.index)
            result_final.to_csv('./DataMining/rpa/df_securities.csv', index=True)
        
    csv_to_js('./DataMining/rpa/df_securities.csv', './DataMining/rpa/df_securities.js')  # Replace 'your_csv_file.csv' with the path to your CSV file

    # Source and destination file paths
    import os
    source_path = './DataMining/rpa'
    dest_path = './DataMining/web_app/static'
    file_name = 'df_securities.js'
    shutil.copyfile(os.path.join(source_path, file_name), os.path.join(dest_path, file_name))
    print(f'File copied from {source_path} to {dest_path} and overwritten if it exists.')
        
        # source_file = './DataMining/rpa/df_securities.js'
        # destination_file = './DataMining/web_app/static/df_securities.js'

        # # Copy the file, overwriting if it exists
        # shutil.copyfile(source_file, destination_file)

        # print(f"File {source_file} copied to {destination_file} and overwritten if it exists.")