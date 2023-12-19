import json
import re  # parsing JSON and regular expressions
from tester import init, test, tester_main
import nbutils
import os

ANSWERS = {}  # global variable to store answers { key = question number, value = output of the answer cell }

FILE_NOT_FOUND = False


@init
def collect_cells(*args, **kwargs):
    global FILE_NOT_FOUND
    if not os.path.exists('p8.ipynb'):
        FILE_NOT_FOUND = True
        return 

    with open("p8.ipynb") as f:
        nb = json.load(f)  # load the notebook as a json object
        cells = nb["cells"]  # get the list of cells from the notebook
        expected_exec_count = 1  # expected execution count of the next cell

        for cell in cells:
            if "execution_count" in cell and cell["execution_count"]:
                exec_count = cell["execution_count"]
                if exec_count != expected_exec_count:
                    raise Exception(
                        f"""
                        Expected execution count {expected_exec_count} but found {exec_count}. 
                        Please do Restart & Run all then save before running the tester.
                        """)
                expected_exec_count = exec_count + 1

            if cell["cell_type"] != "code":
                continue

            if not cell["source"]:
                # if the cell is empty, skip it (empty = it has no source code)
                continue

            # pattern should be #q1 or #Q1 (#q2 or #Q2, etc.)
            m = re.match(r"#[qQ](\d+)(.*)", cell["source"][0].strip())
            if not m:
                continue

            # found a answer cell, add its output to list
            qnum = int(m.group(1))
            notes = m.group(2).strip()
            if qnum in ANSWERS:
                raise Exception(f"Answer {qnum} repeated!")
            # expected qnum = 1 + (max key in ANSWERS dictionary if ANSWERS is not empty else 0)
            expected = 1 + (max(ANSWERS.keys()) if ANSWERS else 0)
            if qnum != expected:
                print(
                    f"Warning: Expected question {expected} next but found {qnum}!")

            # add the output of the answer cell to the ANSWERS dictionary
            ANSWERS[qnum] = cell["outputs"]

    # print("ANSWERS[7] = ", ANSWERS[7])


@test(points=10)
def q1():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 1 in ANSWERS:
        return "ERROR: Answer to question 1 not found"
    outputs = ANSWERS[1]

    output = nbutils.parse_str_output(outputs)
    if not (output == '"55025"' or output == "'55025'"):
        return "Wrong answer"
    

@test(points=10)
def q2():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 2 in ANSWERS:
        return "ERROR: Answer to question 2 not found"
    outputs = ANSWERS[2]

    output = nbutils.parse_dict_int_output(outputs)
    if not nbutils.compare_dict_ints(
        {'48': 254, '13': 159, '51': 133, '21': 120, '29': 115}, 
        output):
        return "Wrong answer"


@test(points=10)
def q3():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 3 in ANSWERS:
        return "ERROR: Answer to question 3 not found"
    outputs = ANSWERS[3]

    output = nbutils.parse_dict_float_output(outputs)
    if not nbutils.compare_dict_floats(
        {'q1': 5.9604644775390625e-05, 'q2': 5.9604644775390625e-05}, 
        output,
        tolerance=0.02):
        return "Wrong answer"


@test(points=10)
def q4():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 4 in ANSWERS:
        return "ERROR: Answer to question 4 not found"
    outputs = ANSWERS[4]
    
    output = nbutils.parse_list_output(outputs)
    if 'p8' not in output:
        return "Wrong answer"


@test(points=10)
def q5():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 5 in ANSWERS:
        return "ERROR: Answer to question 5 not found"
    outputs = ANSWERS[5]
    
    output = nbutils.parse_dict_int_output(outputs)
    if not nbutils.compare_dict_ints(
        {   
            'Milwaukee': 46570,
            'Dane': 38557,
            'Waukesha': 34159,
            'Brown': 15615,
            'Racine': 13007,
            'Outagamie': 11523,
            'Kenosha': 10744,
            'Washington': 10726,
            'Rock': 9834,
            'Winnebago': 9310
        }, 
        output):
        return "Wrong answer"


@test(points=10)
def q6():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 6 in ANSWERS:
        return "ERROR: Answer to question 6 not found"
    outputs = ANSWERS[6]

    output = nbutils.parse_int_output(outputs)
    if output < 1:
        return "Wrong answer. There should be at least 1 application with your chosen income"


@test(points=10)
def q7():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 7 in ANSWERS:
        return "ERROR: Answer to question 7 not found"
    outputs = ANSWERS[7]
    
    output = nbutils.parse_dict_int_output(outputs)

    data = {
        'Sheboygan': 1,
        'Barron': 1,
        'Brown': 1,
        'Bayfield': 1,
        'Columbia': 1,
        'Monroe': 1,
        'Oneida': 1,
        'Dane': 1,
        'Walworth': 1,
        'Jefferson': 1,
        'Door': 1,
        'Sauk': 1,
        'Marinette': 1,
        'Green Lake': 1,
        'Kewaunee': 1,
        'Outagamie': 1
    }

    for county, app_count in data.items():
        if county not in output:
            return f'Wrong answer. Cannot find all counties'
        if output[county] < app_count:
            return f'Wrong answer. These should be at least {app_count} application(s) for {county}'


@test(points=10)
def q8():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 8 in ANSWERS:
        return "ERROR: Answer to question 8 not found"
    outputs = ANSWERS[8]
    
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(0.2916541228802003, output):
        return "Wrong answer"


@test(points=10)
def q9():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 9 in ANSWERS:
        return "ERROR: Answer to question 9 not found"
    outputs = ANSWERS[9]
    
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(0.805777303717665, output):
        return "Wrong answer"


@test(points=10)
def q10():
    if FILE_NOT_FOUND: return "ERROR: File p8.ipynb not found"
    if not 10 in ANSWERS:
        return "ERROR: Answer to question 10 not found"
    outputs = ANSWERS[10]
    
    output = nbutils.parse_float_output(outputs)
    if output < 0.0 or output > 1.0:
        return "Wrong answer"


if __name__ == '__main__':
    tester_main()
