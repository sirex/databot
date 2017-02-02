from sqlalchemy.engine import reflection


def get_pipe_tables(bot):
    tables = set(reflection.Inspector.from_engine(bot.engine).get_table_names())
    for pipe in bot.engine.execute(bot.models.pipes.select()):
        table_name = 't%s' % pipe.id
        if table_name in tables:
            yield pipe
