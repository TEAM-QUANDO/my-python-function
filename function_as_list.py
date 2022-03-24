from math import exp


def exec_func(function_list, p):
    """
    This function passes the value p to each element in the function_list.
    Each element in function_list is a function
    ----------
    function_list : a list of functions
    p : operand
    -------
    output: The results will be stored in a list and will be the output of this main function
    """
    output = []
    for function in function_list:
        output.append(function(p))

    return output


if __name__ == '__main__':
    sample_list = [str, abs,exp,int]
    print(exec_func(sample_list, 10.0001))