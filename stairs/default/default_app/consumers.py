from .app_config import app


@app.consumer()
def print_fib(value, result):
    print("Fibonacci for value %s, is: %s" % (value, result))
