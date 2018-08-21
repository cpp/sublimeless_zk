import os
import sqlite3
from collections import defaultdict

from autobib import Autobib

class Citavi:

    @staticmethod
    def get_authors_by_citekey(citekey, c):
        # Authors
        c.execute('SELECT Person.FirstName, Person.MiddleName, Person.LastName '
                  'FROM Reference '
                  'INNER JOIN ReferenceAuthor ON Reference.ID = ReferenceAuthor.ReferenceID '
                  'INNER JOIN Person ON Person.ID = ReferenceAuthor.PersonID '
                  'WHERE Reference.CitationKey = ?', (citekey,))
        persons = c.fetchall()

        if len(persons) > 2:
            authors = '{} et al.'.format(persons[0][0])  # last et al
        else:
            authors = ' & '.join(x[2] for x in persons)

        return authors

    @staticmethod
    def extract_all_entries(bibfile, unicode_conversion=False):
        """
        Return dict: {citekey: {title, authors, year}}
        """
        entries = defaultdict(lambda: defaultdict(str))
        if not os.path.exists(bibfile):
            print('bibfile not found:', bibfile)
            return {}

        conn = sqlite3.connect(bibfile)
        c = conn.cursor()

        try:
            c.execute('SELECT CitationKey,Title,Year,ReferenceType,ShortTitle FROM Reference')
            results = c.fetchall()
            print('Reading References from Citavi database.')
            for row in results:
                citekey = row[0]
                entries[citekey]['title'] = row[1]
                entries[citekey]['year'] = row[2]
                entries[citekey]['authors'] = Citavi.get_authors_by_citekey(citekey, c)
                # Herausgeber != Autoren
                entries[citekey]['editors'] = ''
                entries[citekey]['type'] = row[3]
                entries[citekey]['shortTitle'] = row[4]

            conn.close()

        except Exception as e:
            print('Unable to read References from Citavi database: %s' % e)
            conn.close()

        return entries


    @staticmethod
    def extract_all_citekeys(bibfile):
        """
        Parse the bibfile and return all citekeys.
        """
        citekeys = set()
        if not os.path.exists(bibfile):
            print('bibfile not found:', bibfile)
            return []

        conn = sqlite3.connect(bibfile)
        c = conn.cursor()

        try:
            c.execute('SELECT CitationKey FROM Reference')
            results = c.fetchall()
            print('Reading CitationKeys from Citavi database.')
            for row in results:
                citekeys.add(row[0])

            conn.close()

        except Exception as e:
            print('Unable to read References from Citavi database: %s' % e)
            conn.close()

        return citekeys

    @staticmethod
    def get_short_title_by_citekey(bibfile,citekey):
        conn = sqlite3.connect(bibfile)
        c = conn.cursor()

        shortTitle = ''

        try:
            citekey = citekey.replace('@', '', 1)
            c.execute('SELECT ShortTitle FROM Reference WHERE CitationKey=?', (citekey,))
            result = c.fetchone()
            shortTitle = result[0]

            conn.close()

        except Exception as e:
            print('Unable to read References from Citavi database: %s' % e)
            conn.close()

        return shortTitle

    @staticmethod
    def get_title_by_citekey(bibfile, citekey):
        conn = sqlite3.connect(bibfile)
        c = conn.cursor()

        title = ''

        try:
            citekey = citekey.replace('@', '', 1)
            c.execute('SELECT Title FROM Reference WHERE CitationKey=?', (citekey,))
            result = c.fetchone()
            title = result[0]

            conn.close()

        except Exception as e:
            print('Unable to read References from Citavi database: %s' % e)
            conn.close()

        return title

    @staticmethod
    def get_bib_by_citekey(bibfile, citekey):
        conn = sqlite3.connect(bibfile)
        c = conn.cursor()

        bib = ''

        try:
            citekey = citekey.replace('@', '', 1)
            c.execute('SELECT Title, ShortTitle FROM Reference WHERE CitationKey=?', (citekey,))
            result = c.fetchone()
            part = str.split(result[1], '–', 1)
            bib = '{}– {}'.format(part[0], result[0])
            conn.close()

        except Exception as e:
            print('Unable to read References from Citavi database: %s' % e)
            conn.close()

        return bib

    @staticmethod
    def create_bibliography(text, bibfile, pandoc='pandoc'):
        """
        Create a bibliography for all citations in text in form of a dictionary.
        """
        citekeys = Citavi.extract_all_citekeys(bibfile)
        if not citekeys:
            return {}
        citekeys = Autobib.find_citations(text, citekeys)
        citekey2bib = {}
        for citekey in citekeys:
            pandoc_input = citekey.replace('#', '@', 1)
            #pandoc_out = Autobib.run(pandoc, bibfile, pandoc_input)
            #citation, bib = Autobib.parse_pandoc_out(pandoc_out)
            citekey2bib[citekey] = Citavi.get_bib_by_citekey(bibfile, citekey)
        return citekey2bib
