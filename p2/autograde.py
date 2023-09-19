import json, re

from tester import init, test, tester_main
import nbutils

ANSWERS = {}

def parse_question_config(c):
    if c.startswith("run="):
        return {"run": c[4:]}
    config = {}
    opts = c.split(",")
    for opt in opts:
        parts = opt.split("=")
        if len(parts) != 2:
            continue
        config[parts[0]] = parts[1].strip()
    return config


@init
def collect_cells():
    with open("nb/p2.ipynb") as f:
        nb = json.load(f)
        cells = nb["cells"]
        expected_exec_count = 1

        for cell in cells:
            if "execution_count" in cell and cell["execution_count"]:
                exec_count = cell["execution_count"]
                if exec_count != expected_exec_count:
                    raise Exception(f"Expected execution count {expected_exec_count} but found {exec_count}. Please do Restart & Run all then save before running the tester.")
                expected_exec_count = exec_count + 1

            if cell["cell_type"] != "code":
                continue
            if not cell["source"]:
                continue
            m = re.match(r"#[qQ](\d+)(.*)", cell["source"][0].strip())
            if not m:
                continue
            
            # found a answer cell, add its output to list
            qnum = int(m.group(1))
            notes = m.group(2).strip()
            if qnum in ANSWERS:
                raise Exception(f"Answer {qnum} repeated!")
            expected = 1 + (max(ANSWERS.keys()) if ANSWERS else 0)
            if qnum != expected:
                print(f"Warning: Expected question {expected} next but found {qnum}!")

            ANSWERS[qnum] = cell["outputs"]

@test(points=10)
def q1():
    if not 1 in ANSWERS:
        raise Exception("Answer to question 1 not found")
    outputs = ANSWERS[1]
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(83520, output):
        return "Wrong answer"

@test(points=10)
def q2():
    if not 2 in ANSWERS:
        raise Exception("Answer to question 2 not found")
    outputs = ANSWERS[2]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(0.0, output):
        return "Wrong answer"
    
@test(points=10)
def q3():
    if not 3 in ANSWERS:
        raise Exception("Answer to question 3 not found")
    outputs = ANSWERS[3]
    output = nbutils.parse_bool_output(outputs)
    if not nbutils.compare_bool(False, output):
        return "Wrong answer"

@test(points=10)
def q4():
    if not 4 in ANSWERS:
        raise Exception("Answer to question 4 not found")
    outputs = ANSWERS[4]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(9.844, output):
        return "Wrong answer"

@test(points=10)
def q5():
    if not 5 in ANSWERS:
        raise Exception("Answer to question 5 not found")
    outputs = ANSWERS[5]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(12.073632183908048, output):
        return "Wrong answer"

@test(points=10)
def q6():
    if not 6 in ANSWERS:
        raise Exception("Answer to question 6 not found")
    outputs = ANSWERS[6]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(19.0, output):
        return "Wrong answer"
    
@test(points=10)
def q7():
    if not 7 in ANSWERS:
        raise Exception("Answer to question 7 not found")
    outputs = ANSWERS[7]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(3.999999523162842, output):
        return "Wrong answer"
    
@test(points=10)
def q8():
    if not 8 in ANSWERS:
        raise Exception("Answer to question 8 not found")
    outputs = ANSWERS[8]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(197.8007662835249, output):
        return "Wrong answer"
    
@test(points=10)
def q9():
    if not 9 in ANSWERS:
        raise Exception("Answer to question 9 not found")
    outputs = ANSWERS[9]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(26.8113940147193, output):
        return "Wrong answer"

@test(points=10)
def q10():
    if not 10 in ANSWERS:
        raise Exception("Answer to question 10 not found")
    outputs = ANSWERS[10]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(29.05854692548551, output):
        return "Wrong answer"

if __name__ == '__main__':
    tester_main()