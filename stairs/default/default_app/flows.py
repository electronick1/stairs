

def calculate_fibonacci(value):
    if value == 0:
        return dict(result=0)
    elif value == 1:
        return dict(result=1)

    return dict(
        result=calculate_fibonacci(value - 1)['result'] +
               calculate_fibonacci(value - 2)['result']
    )
