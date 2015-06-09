import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, mysql, sqlite

BigInteger = sa.BigInteger()
BigInteger = BigInteger.with_variant(postgresql.BIGINT(), 'postgresql')
BigInteger = BigInteger.with_variant(mysql.BIGINT(), 'mysql')
BigInteger = BigInteger.with_variant(sqlite.INTEGER(), 'sqlite')


metadata = sa.MetaData()

tasks = sa.Table(
    'databottasks', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('bot', sa.String(255), nullable=False),
    sa.Column('task', sa.String(255), nullable=False),
)

state = sa.Table(
    'databotstate', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('source_id', sa.Integer, sa.ForeignKey(tasks.c.id)),
    sa.Column('target_id', sa.Integer, sa.ForeignKey(tasks.c.id), nullable=False),
    sa.Column('offset', sa.Integer, nullable=False)
)

errors = sa.Table(
    'databoterrors', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('state_id', sa.Integer, sa.ForeignKey(state.c.id), nullable=False),
    sa.Column('row_id', BigInteger, nullable=False),
    sa.Column('retries', sa.Integer, default=0),
    sa.Column('traceback', sa.UnicodeText, default='', nullable=False),
    sa.Column('created', sa.DateTime),
    sa.Column('updated', sa.DateTime)
)


def get_data_table(name, meta):
    return sa.Table(
        name, meta,
        sa.Column('id', BigInteger, primary_key=True),
        sa.Column('key', sa.Unicode(255), index=True, default='', nullable=False),
        sa.Column('value', sa.LargeBinary, default='', nullable=False),
        sa.Column('created', sa.DateTime),
    )
