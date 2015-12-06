import datetime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, mysql, sqlite

BigInteger = sa.BigInteger()
BigInteger = BigInteger.with_variant(postgresql.BIGINT(), 'postgresql')
BigInteger = BigInteger.with_variant(mysql.BIGINT(), 'mysql')
BigInteger = BigInteger.with_variant(sqlite.INTEGER(), 'sqlite')


class Models(object):

    def __init__(self, metadata):
        self.metadata = metadata

        self.pipes = sa.Table(
            'databotpipes', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('bot', sa.String(255), nullable=False),
            sa.Column('pipe', sa.String(255), nullable=False),
        )

        self.state = sa.Table(
            'databotstate', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('source_id', sa.Integer, sa.ForeignKey(self.pipes.c.id)),
            sa.Column('target_id', sa.Integer, sa.ForeignKey(self.pipes.c.id), nullable=False),
            sa.Column('offset', sa.Integer, nullable=False)
        )

        self.errors = sa.Table(
            'databoterrors', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('state_id', sa.Integer, sa.ForeignKey(self.state.c.id), nullable=False),
            sa.Column('row_id', BigInteger, nullable=False),
            sa.Column('retries', sa.Integer, default=0),
            sa.Column('traceback', sa.UnicodeText, default='', nullable=False),
            sa.Column('created', sa.DateTime),
            sa.Column('updated', sa.DateTime)
        )

        self.migrations = sa.Table(
            'databotmigrations', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.String(255), nullable=False),
            sa.Column('created', sa.DateTime),
        )

    def get_data_table(self, name):
        return sa.Table(
            name, self.metadata,
            sa.Column('id', BigInteger, primary_key=True),
            sa.Column('key', sa.Unicode(40), index=True, default='', nullable=False),
            sa.Column('value', sa.LargeBinary, default='', nullable=False),
            sa.Column('created', sa.DateTime, default=datetime.datetime.utcnow),
        )
