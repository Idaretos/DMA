import os
from sqlalchemy import create_engine, MetaData
from sqlacodegen.codegen import CodeGenerator
from graphviz import Source

# Read MySQL password from file
with open('password.txt', 'r') as file:
    password = file.read()

team = 10

# Connect to MySQL database
engine = create_engine(f'mysql://root:{password}@localhost:3306/DMA_team{team}')
metadata = MetaData(bind=engine)

# Generate SQLAlchemy models
codegen = CodeGenerator(metadata)
models = codegen.render()

# Generate ERD from SQLAlchemy models
if models is not None:
    erd = Source(models)
    erd.render('database_schema', format='png', cleanup=True)  # Output ERD as PNG file

# Suppress SQLAlchemy deprecation warning
os.environ['SQLALCHEMY_SILENCE_UBER_WARNING'] = '1'
