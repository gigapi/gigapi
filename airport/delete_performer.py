import os


class FSDeletePerformer:
    def __init__(self, base: str, database: str, schema: str, table: str):
        self.base = base
        self.database = database
        self.schema = schema
        self.table = table

    def do_delete(self, delete_plan: str):
        p = os.path.join(self.base, self.database, self.schema, self.table, delete_plan)
        if os.path.exists(p):
            os.remove(p)