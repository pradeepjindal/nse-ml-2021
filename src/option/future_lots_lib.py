import csv


def get_future_data():
    symbol_to_lot_map = {}
    with open('fm-lots.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                # print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                # print(f'\t{row[0]} = {row[1]}.')
                symbol_to_lot_map[row[0]] = int(row[1])
                line_count += 1
        print(f'Processed {line_count} lines.')
    return symbol_to_lot_map
