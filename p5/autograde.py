import json, re # parsing JSON and regular expressions

from tester import init, test, tester_main
import nbutils

ANSWERS = {} # global variable to store answers { key = question number, value = output of the answer cell }


@init
def collect_cells():
    with open("nb/p5.ipynb") as f:
        nb = json.load(f) # load the notebook as a json object
        cells = nb["cells"] # get the list of cells from the notebook
        expected_exec_count = 1 # expected execution count of the next cell

        for cell in cells:
            if "execution_count" in cell and cell["execution_count"]:
                exec_count = cell["execution_count"]
                if exec_count != expected_exec_count:
                    raise Exception(f"Expected execution count {expected_exec_count} but found {exec_count}. Please do Restart & Run all then save before running the tester.")
                expected_exec_count = exec_count + 1

            if cell["cell_type"] != "code":
                continue

            if not cell["source"]: 
                # if the cell is empty, skip it (empty = it has no source code)
                continue

            m = re.match(r"#[qQ](\d+)(.*)", cell["source"][0].strip()) # pattern should be #q1 or #Q1 (#q2 or #Q2, etc.)
            if not m:
                continue
            
            # found a answer cell, add its output to list
            qnum = int(m.group(1)) 
            notes = m.group(2).strip() 
            if qnum in ANSWERS:
                raise Exception(f"Answer {qnum} repeated!")
            expected = 1 + (max(ANSWERS.keys()) if ANSWERS else 0) # expected qnum = 1 + (max key in ANSWERS dictionary if ANSWERS is not empty else 0)
            if qnum != expected:
                print(f"Warning: Expected question {expected} next but found {qnum}!")

            ANSWERS[qnum] = cell["outputs"] # add the output of the answer cell to the ANSWERS dictionary

    # print("ANSWERS[7] = ", ANSWERS[7])



@test(points=10)
def q1():
    if not 1 in ANSWERS:
        raise Exception("Answer to question 1 not found")
    outputs = ANSWERS[1]
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(525, output):
        return "Wrong answer"

@test(points=10)
def q2():
    if not 2 in ANSWERS:
        raise Exception("Answer to question 2 not found")
    outputs = ANSWERS[2]
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(525, output):
        return "Wrong answer"

@test(points=10)
def q3():
    if not 3 in ANSWERS:
        raise Exception("Answer to question 3 not found")
    outputs = ANSWERS[3]
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(525, output):
        return "Wrong answer"


@test(points=10) # desugars to test(points=10)(q1) = wrapper(q1) -> TESTS["q1"] = _unit_test(q1, 10, None, "") 
def q4():
    if not 4 in ANSWERS:
        raise Exception("Answer to question 4 not found")
    outputs = ANSWERS[4]
    # print("ANSWERS[4] = ", ANSWERS[4])
    output = nbutils.parse_dict_bool_output(outputs)
    
    if not nbutils.compare_dict_bools(
        {'banks': False, 
        'loans': False, 
        'action_taken': True, 
        'counties': True, 
        'denial_reason': True, 
        'ethnicity': True, 
        'loan_purpose': True, 
        'loan_type': True, 
        'preapproval': True, 
        'property_type': True, 
        'race': True, 
        'sex': True, 
        'states': True, 
        'tracts': True
        }, output):
        return "Wrong answer"


@test(points=10)
def q5():
    if not 5 in ANSWERS:
        raise Exception("Answer to question 5 not found")
    outputs = ANSWERS[5]
    # print("test 5 outputs: ", outputs)
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(19739, output):
        return "Wrong answer"

@test(points=10)
def q6():
    if not 6 in ANSWERS:
        raise Exception("Answer to question 6 not found")
    # to be manually graded


@test(points=10)
def q7():
    if not 7 in ANSWERS:
        raise Exception("Answer to question 7 not found")
    outputs = ANSWERS[7]
    # print("ANSWERS[7] = ", ANSWERS[7])
    output = nbutils.parse_dict_float_output(outputs)
    
    if not nbutils.compare_dict_floats(
    {'Milwaukee': 3.1173465727097907,
    'Waukesha': 2.8758225602027756,
    'Washington': 2.851009389671362,
    'Dane': 2.890674955595027,
    'Brown': 3.010949119373777,
    'Racine': 3.099783715012723,
    'Outagamie': 2.979661835748792,
    'Winnebago': 3.0284761904761908,
    'Ozaukee': 2.8673765432098772,
    'Sheboygan': 2.995511111111111
    }, output):
        return "Wrong answer"
        

@test(points=10)
def q8():
    if not 8 in ANSWERS:
        raise Exception("Answer to question 8 not found")
    # to be manually graded
    
    
@test(points=10)
def q9():
    if not 9 in ANSWERS:
        raise Exception("Answer to question 9 not found")
    outputs = ANSWERS[9]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(242868.0, output):
        return "Wrong answer"

@test(points=10)
def q10():
    if not 10 in ANSWERS:
        raise Exception("Answer to question 10 not found")
    outputs = ANSWERS[10]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.is_accurate(0.88, output):
        return "Wrong answer"

if __name__ == '__main__': 
    tester_main()
