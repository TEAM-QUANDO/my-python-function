def prefix_hello(name):
    """
    User Defined function to prefix hello to the name
    :param name: name
    :return: Returns the name after prefixing hello
    """
    return "Hello " + str(name)


def exec_func(function, operant_list):
    """
    This function passes the each value in the operant list to the function.
    ----------
    function : function (built-in or user defined)
    operant_list : list of operand
    -------
    output: The results will be stored in a list and will be the output of this main function
    """
    output = []
    for operant in operant_list:
        output.append(function(operant))

    return output


if __name__ == '__main__':
    # Example with built-in function int
    value_list = [10.001, 190.1, 21.1, 20, 22, 24.5]
    print(exec_func(int, value_list))

    # Example with user defined function prefix_hello
    name_list = ["amal", "sabitha", "edward"]
    print(exec_func(prefix_hello, name_list))
