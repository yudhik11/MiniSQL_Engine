from __future__ import print_function
import csv, sys
import sqlparse
import operator
import itertools
from copy import deepcopy

database = {}
aggregate_fn = ["max", "sum", "count", "min", "avg"]
cond_join, lft_ptr, right_ptr= False, -1, -1
conditions = [">=", "<=", ">", "<", "="]

def get_operator_fn(op):
    return {
        '>' : operator.gt,
        '<' : operator.lt,
        '>=' : operator.ge,
        '<=' : operator.le,
        '=' : operator.eq,
        '!=' : operator.ne,
        }[op]

def str2intf(num):
    try:
        return int(num)
    except:
        return float(num)

def check_type(val):
    try:
        return str2intf(val)
    except:
        return "str"

def swap(a, b):
    return b,a

def get_schema():
    with open('./files/metadata.txt', 'r') as f:
        file = f.readlines()
    table_open = 0
    table_name = ""
    for line in file:
        line = line.strip()
        if line == "<begin_table>":
            table_open = 1
        elif line == "<end_table>":
            table_open = 0
        elif table_open == 1:
            table_open = 2
            table_name = line
            database[table_name] = {}
            database[table_name]['attrs'] = []
        elif table_open == 2:
            # line = line.lower()
            database[table_name]['attrs'].append(table_name+'.'+line)
    fill_schema()
    # print( database)

def convert_int(x):
    return int(x.replace('"', '').replace('\'', ''))

def fill_schema():
    for key in database.keys():
        with open('./files/' + str(key) + '.csv') as f:
            file = f.readlines()
            database[key]['data'] = []
        for line in file:
            # print (line.strip().split(','))
            # print( [int(field.strip('"')) for field in line.strip().split(',')])
            database[key]['data'].append([convert_int(x) for x in line.strip().split(',')])

def parser(command):
    command = ' '.join(command.split())
    try:
        query = sqlparse.parse(command)
    except:
        sys.exit("Inccorect form of query.")
    cmd =[]
    for token in query[0].tokens:
        if str(token) == ' ':
            continue
        cmd.append(str(token))
    if "select" not in command.lower() or "from" not in command.lower():
        sys.exit("Incorrect form of query.")
    # print(cmd)
    if cmd[-1][-1] != ';':
        sys.exit("Incorrect form of query as Colon not present")
    if cmd[-1] == ';':
        cmd.pop()
    else:
        cmd[-1] = cmd[-1][:-1]
    attributes = []
    tables = []
    conditional = []
    flag=0
    for keyword in cmd:
        # print( keyword)
        if (keyword.lower() == "select"):
            flag = 1
        elif (keyword.lower() == "from"):
            flag = 2
        else:
            if flag==1:
                for x in keyword.strip().split(','):
                    attributes.append(x.replace(' ', ''))
            elif flag == 2:
                for x in keyword.strip().split(','):
                    tables.append(x.replace(' ', ''))
                    flag=3
            elif flag == 3:
                if "where" in keyword.lower():
                    conditional.append(keyword.lstrip().rstrip().replace("where ", ''))
                else : 
                    sys.exit("Incorrect form of query.")
        
            # else:
            #     conditional.append(keyword)
    return tables, attributes, conditional
        
    # print( parse)

def get_table(tables):
    table_header = []
    final_table = []
    for table in tables:
        if table not in database:
            sys.exit("Unknown table as " + table + " does not exist.")
        if len(table_header) == 0:
            table_header = table_header + database[table]['attrs']
            final_table = database[table]['data']
        else:
            table_header = table_header + database[table]['attrs']
            temp_table =  database[table]['data']
            # print( np.array(final_table).shape)
            # print( np.array(temp_table).shape)
            
            temp_table = list(itertools.product(final_table, temp_table))
            # print( np.array(temp_table).shape)
            # print( temp_table)
            final_table = []

            for i in temp_table:
                tmp = deepcopy(i[0])
                for elem in i[1]:
                    tmp.append(elem)
                final_table.append(tmp)
    
    return table_header, final_table

def cond_split(condition):
    for cond in conditions:
        if cond in condition:
            condition = condition.split(cond)
            return condition[0], cond, condition[1]

def check_cond(l, sign, r):
    # print(get_operator_fn(sign))
    return get_operator_fn(sign)(l, r)

def conditional_join(condition, table_cols, final_table):
    global cond_join, lft_ptr, right_ptr
    lhs_c, sign, rhs_c = condition
    if sign == "=":
        cond_join = True
    for idx, col in enumerate(table_cols):
        if lhs_c == col or lhs_c == col.split('.')[-1]:
            if lft_ptr != -1:
                sys.exit("Ambiguous: Column in conditions present in various tables")
            lft_ptr = idx
        if rhs_c == col or rhs_c == col.split('.')[-1]:
            if right_ptr != -1:
                sys.exit("Ambiguous: Column in conditions present in various tables")
            right_ptr = idx

    if lft_ptr == (-1) or right_ptr == (-1):
        sys.exit("Condition Variables are not present in the table")
    temp = []

    for row in final_table:
        if check_cond(row[lft_ptr], sign, row[right_ptr]):
            temp.append(row)
    return temp, table_cols

def switch(chr):
    if chr == "<":
        return ">"
    elif chr == ">":
        return "<"
    return chr

def switch_sign(sign):
    tmp = ""
    for chr in sign:
        tmp = tmp+switch(chr)
    return tmp

def conditioned_table(table_cols, final_table, cond1, cond2, add_type = None):
    # print(table_cols, final_table, cond1, cond2, add_type)
    flag1, flag2 = False, False

    if add_type == None:
        cond2 = cond1
        add_type = "and"
    for cond in conditions:
        if cond in cond1:
            flag1 = True
        if cond in cond2:
            flag2 = True
    if (flag1 and flag2 )== False:
        sys.exit("Wrong condition format")
    
    left_cond1, sign_cond1, right_cond1 = cond_split(cond1)
    type_lhsc1, type_rhsc1 = check_type(left_cond1), check_type(right_cond1)
    left_cond2, sign_cond2, right_cond2 = cond_split(cond2)
    type_lhsc2, type_rhsc2 = check_type(left_cond2), check_type(right_cond2)
    # print(left_cond1, right_cond2, left_cond1, right_cond2)
    # print(type_lhsc1, type_rhsc1, type_lhsc2, type_rhsc2)
    
    if type_lhsc1 == type_rhsc1:
        if type_lhsc1 != "str":
            sys.exit("Condition format not supported")
        else:
            temp, table_cols = conditional_join((left_cond1, sign_cond1, right_cond1), table_cols, final_table)
            return temp, table_cols
    if type_lhsc2 == type_rhsc2:
        if type_lhsc2 != "str":
            sys.exit("Condition format not supported")
    
    if type_lhsc1!="str":
        left_cond1, right_cond1 = swap(left_cond1, right_cond1)
        sign_cond1 = switch_sign(sign_cond1)

    if type_lhsc2!="str":
        left_cond2, right_cond2 = swap(left_cond2, right_cond2)
        sign_cond2 = switch_sign(sign_cond2)
    right_cond1 = int(right_cond1)
    right_cond2 = int(right_cond2)

    index_c1, index_c2 = -1, -1
    # cnt_c1
    
    for idx, col in enumerate(table_cols):
        if left_cond1 == col or left_cond1 == col.split('.')[-1]:
            if index_c1 != -1:
                sys.exit("Ambiguous: Column in conditions present in various tables")
            index_c1 = idx
        if left_cond2 == col or left_cond2 == col.split('.')[-1]:
            if index_c2 != -1:
                sys.exit("Ambiguous: Column in conditions present in various tables")
            index_c2 = idx
    if (index_c1 == -1) or (index_c2 == -1):
        sys.exit("Condition Variables are not present in the table")
    temp = []
    
    if add_type.lower() == "and":
        for row in final_table:
            if check_cond(row[index_c1], sign_cond1, right_cond1) and check_cond(row[index_c2], sign_cond2, right_cond2):
                temp.append(row)
    elif add_type.lower() == "or":
        for row in final_table:
            if check_cond(row[index_c1], sign_cond1, right_cond1) or check_cond(row[index_c2], sign_cond2, right_cond2):
                temp.append(row)			

    return temp, table_cols

def get_column(table, idx):
    tmp = list(zip(*table))
    if (len(tmp) > idx):
        return tmp[idx]
    return []

def aggregate_table(tables, table_header, final_table, attr):
    # print(len(tables), table_header, final_table, attr)
    attr= attr.split('(')
    colname = attr[1]
    attr = attr[0].replace(' ', '')
    colname = colname.replace(')', '').replace(' ', '')
    if '.' not in colname:
        colname = tables[0] + '.' + colname
    # print(table_header)
    if table_header.count(colname) == 0:
        sys.exit("Column " + colname + " not present in the table")
    idx = table_header.index(colname)
    if attr not in aggregate_fn:
        sys.exit("Invalid attribute")
    # column = list(zip(*final_table)[idx])
    column = get_column(final_table, idx)
    # print(column)
    print(str(attr) + '(' + str(colname) + ')')
    if attr == "max":
        print(max(column))
    elif attr == "min":
        print(min(column))
    elif attr == "sum":
        print(sum(column))
    elif attr == "count":
        print(len(column))
    elif attr == "avg":
        print(sum(column)*1.0/len(column))
    else:
        sys.exit("Unexpected error")
    return True

def distinct_query(tmp_table):
    distinct_table = []
    for tp in tmp_table:
        if tp not in distinct_table:
            distinct_table.append(tp)
    return distinct_table

def selected_col(table, table_header, final_table, attributes):
    # print(attributes)
    tmp_table = []
    copy_attr = []
    if attributes[0] == "*":
        attributes = table_header
    # print(attributes)
    for attr in attributes:
        if attr.lower() == "distinct":
            continue
        tmp = attr
        tmprr = tmp
        cntr = 0
        if '.' not in tmp:
            for header in table_header:
                if tmp == header.split('.')[-1]:
                    tmprr = str(header.split('.')[0]) + '.' + tmp
                    cntr = cntr+1
            if cntr>1:
                sys.exit("Ambigous Case: Column present in multiple tables")
            if cntr == 0:
                sys.exit("Column name not found in any table")
        tmp = tmprr
        copy_attr.append(tmp)
        if table_header.count(tmp) == 0:
            sys.exit("Column " + tmp + " not present in the table")
        idx = table_header.index(tmp)
        # print(idx, tmp)
        tmp_table.append(get_column(final_table, idx))
        # print(tmp_table)
    # print(distinct_table)
    return tmp_table, copy_attr

def projected_table(command, tables, table_header, final_table, attributes, conditional):
    cnt = 0
    for attr in attributes:
        cnt = cnt + attr.count('(') 
    if (cnt > 1):
        sys.exit("Usage: No more than one aggregate function can be provided")
    else:
        cnt = 0
    if len(attributes) == 1 and '(' in attributes[0]:
        attr = attributes[0]
        return aggregate_table(tables, table_header, final_table, attr)
    flag =0
    add_type = None
    if (len(conditional) > 0):
        cond1, cond2 = '', ''
        conditional = conditional[0].split()
        for elem in conditional:
            if flag : cond2 = cond2 +elem
            elif elem.lower() == "and" or elem.lower() == "or":
                add_type = elem
                flag=1
            if flag == 0: cond1 = cond1 + elem
        final_table, table_header = conditioned_table(table_header, final_table, cond1, cond2, add_type)
    tmp_table, attributes = selected_col(tables, table_header, final_table, attributes)
    tmp_table = zip(*tmp_table)
    final_table = []
    for tp in tmp_table:
        final_table.append(list(tp))
    
    # print_table(final_table, attributes)

    if "distinct" in command.lower().split():
        # print("dis")
        # if len(tables) > 1:
        #     sys.exit("Invalid query: More than one table provided with Distict query")
        final_table = distinct_query(final_table)
    print_table(final_table, attributes, command)
    return True

def print_table(final_table, table_header, command):
    global cond_join, lft_ptr, right_ptr
    star = ("*" in command.split())
    # print(star)
    flag=0
    if star and cond_join:
        flag=max(lft_ptr, right_ptr)
        del table_header[flag]
        for j in range(0, len(final_table)):
            del final_table[j][flag]
    
    print(','.join(table_header))
    
    for j in range(0, len(final_table)):
        for i in range(0, len(final_table[j])):
            if i == len(table_header) -1:
                print(final_table[j][i])
            else:
                print(final_table[j][i], end=",")

def main(command):
    # print(command)
    get_schema()

    tables, attributes, condintional = parser(command)
    # print( tables, attributes, condintional)
    table_header, final_table = get_table(tables)
    # print_table(final_table, table_header)
    projected_table(command, tables, table_header, final_table, attributes, condintional)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python filename.py \"command\"")
    command = sys.argv[1]
    main(command)
