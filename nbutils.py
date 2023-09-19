import math

def parse_str_output(outputs):
    outputs = [o for o in outputs if o.get("output_type") == "execute_result"]
    if len(outputs) == 0:
        raise Exception("Output not found")
    elif len(outputs) > 1:
        raise Exception("Too many outputs")
    return "".join(outputs[0]["data"]["text/plain"]).strip()

def parse_int_output(outputs):
    return int(parse_str_output(outputs))

def parse_float_output(outputs):
    return float(parse_str_output(outputs))

def parse_bool_output(outputs):
    eval_output = eval(parse_str_output(outputs))
    if type(eval_output) is not bool:
        raise Exception("Error parsing output as bool")
    return eval_output

def compare_bool(expected, actual):
    return expected == actual

def compare_int(expected, actual):
    return expected == actual

def compare_type(expected, actual):
    return expected == actual

def compare_float(expected, actual, tolerance = 0.01):
    if math.isnan(expected) and math.isnan(actual):
        return True
    return math.isclose(expected, actual, rel_tol=tolerance)

def compare_str(expected, actual, case_sensitive=True):
    if not case_sensitive:
        return expected.upper() == actual.upper()
    return expected == actual

def compare_list(expected, actual, strict_order=True):
    if strict_order:
        return expected == actual
    else:
        return sorted(expected) == sorted(actual)

def compare_tuple(expected, actual):
    return expected == actual

def compare_set(expected, actual, superset = False):
    if superset:
        return len(expected - actual) == 0
    else:
        return expected == actual

def compare_dict(expected, actual, tolerance = 0.01):
    if tolerance:
        if expected.keys() != actual.keys():
            return False

        for key in expected.keys():
            if not compare_float(expected[key], actual[key], tolerance):
                return False
                
        return True

    return expected == actual

def compare_figure(expected, actual):
    return type(expected) == type(actual)