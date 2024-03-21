import csv

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