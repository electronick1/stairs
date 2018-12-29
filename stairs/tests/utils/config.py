
redis_host = 'localhost'
redis_port = 6379


apps = [
]

generators = [
    'stairs.generators.sql_adapter',
    'stairs.generators.file_adapter'
]


worker = 'redis'  # salary, rq, kafka

