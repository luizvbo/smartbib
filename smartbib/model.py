from sqlalchemy import (
    Table, String, Text, Integer, Column, ForeignKey, MetaData
)
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.dialects.mysql import insert, INTEGER, BIGINT
from sqlalchemy.sql import select, update


class PaperDatabase:
    def __init__(self) -> None:
        self.metadata_obj = MetaData()
        self.paper = self._gen_table_paper()
        self.pdf_url = self._gen_table_pdf_url()
        self.fos = self._gen_table_fos()
        self.author = self._gen_table_author()
        #  self.authorship = self._gen_table_authorship()
        self.citation = self._gen_table_citation()

    def _gen_table_paper(self):
        return Table(
            'papers', self.metadata_obj,
            Column('id_1', BIGINT(unsigned=True), primary_key=True),
            Column('id_2', BIGINT(unsigned=True), primary_key=True),
            Column('id_3', INTEGER(unsigned=True), primary_key=True),
            Column('title', String(512)),
            Column('paper_abstract', Text),
            Column('year', Integer),
            Column('s2_url', String(256)),
            Column('venue', String(256)),
            Column('journal_name', String(256)),
            Column('journal_volume', String(256)),
            Column('journal_pages', String(256)),
        )

    def _gen_table_pdf_url(self):
        return Table(
            'pdf_urls', self.metadata_obj,
            Column('id_paper_1', BIGINT(unsigned=True)),
            Column('id_paper_2', BIGINT(unsigned=True)),
            Column('id_paper_3', INTEGER(unsigned=True)),
            Column('content', Text),
            ForeignKeyConstraint(
                ['id_paper_1', 'id_paper_2', 'id_paper_3'],
                ['papers.id_1', 'papers.id_2', 'papers.id_3']
            )
        )

    def _gen_table_fos(self):
        return Table(
            'fields_of_study', self.metadata_obj,
            Column('id_paper_1', BIGINT(unsigned=True)),
            Column('id_paper_2', BIGINT(unsigned=True)),
            Column('id_paper_3', INTEGER(unsigned=True)),
            Column('content', String(40)),
            ForeignKeyConstraint(
                ['id_paper_1', 'id_paper_2', 'id_paper_3'],
                ['papers.id_1', 'papers.id_2', 'papers.id_3']
            )

        )

    def _gen_table_author(self):
        return Table(
            'authors', self.metadata_obj,
            Column('id_author', Integer),
            Column('name', String(256)),
            Column('id_paper_1', BIGINT(unsigned=True)),
            Column('id_paper_2', BIGINT(unsigned=True)),
            Column('id_paper_3', INTEGER(unsigned=True)),
            ForeignKeyConstraint(
                ['id_paper_1', 'id_paper_2', 'id_paper_3'],
                ['papers.id_1', 'papers.id_2', 'papers.id_3']
            )
        )

    #  def _gen_table_authorship(self):
    #      return Table(
    #          'authorship', self.metadata_obj,
    #          Column('id_author', Integer, ForeignKey('authors.id')),
    #          Column('id_paper_1', BIGINT(unsigned=True)),
    #          Column('id_paper_2', BIGINT(unsigned=True)),
    #          Column('id_paper_3', INTEGER(unsigned=True)),
    #          ForeignKeyConstraint(
    #              ['id_paper_1', 'id_paper_2', 'id_paper_3'],
    #              ['papers.id_1', 'papers.id_2', 'papers.id_3']
    #          )
    #
    #      )

    def _gen_table_citation(self):
        return Table(
            'citations', self.metadata_obj,
            Column('id_cited_1', BIGINT(unsigned=True)),
            Column('id_cited_2', BIGINT(unsigned=True)),
            Column('id_cited_3', INTEGER(unsigned=True)),
            Column('id_citer_1', BIGINT(unsigned=True)),
            Column('id_citer_2', BIGINT(unsigned=True)),
            Column('id_citer_3', INTEGER(unsigned=True)),
            ForeignKeyConstraint(
                ['id_cited_1', 'id_cited_2', 'id_cited_3'],
                ['papers.id_1', 'papers.id_2', 'papers.id_3']
            )
        )


    def insert(self, table, ignore_dup=False):
        insert_clause = insert(table)
        if ignore_dup:
            insert_clause = insert_clause.prefix_with('IGNORE')
        return insert_clause

    def select_where_in(self, table, column, values, selected_columns=None):
        if selected_columns:
            select_clause = select(
                [getattr(table.c, column) for column in selected_columns]
            )
        else:
            select_clause = select(table)
        return select_clause.where(
            getattr(table.c, column).in_(values)
        )

    def update_where_equal(self, table, eq_column, eq_value, values):
        update_clause = update(table).where(
            getattr(table.c, eq_column) == eq_value
        ).values(values)
        return update_clause

    def create_tables(self, db_engine):
        self.metadata_obj.create_all(db_engine)

