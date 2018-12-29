from .app_config import app


@app.consumer()
def print_smth(**data):
    print("output")
    return data
