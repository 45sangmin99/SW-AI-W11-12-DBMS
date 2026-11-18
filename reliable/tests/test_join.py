#!/usr/bin/env python3
"""JOIN through the CLI, including duplicates, result processing and overlays."""
from pathlib import Path
import tempfile

from helpers import fixture, run


def main():
    with tempfile.TemporaryDirectory(prefix='mdb-join-') as directory:
        path = Path(directory)
        fixture(path)
        (path / 'characters.csv').write_text('1,Ana,10\n2,Bo,20\n3,Cam,10\n4,Dia,99\n')
        (path / 'items.csv').write_text('1,Hat,10\n2,Coat,10\n3,Shoe,20\n4,Bag,40\n')
        join = 'FROM characters c JOIN items AS i ON i.price = c.attack'

        def rows(sql, expected, names=None):
            _, results = run(path, sql)
            result = results[-1]
            assert result['rows'] == expected, (sql, result)
            if names is not None:
                assert [column['name'] for column in result['columns']] == names, result

        rows('SELECT c.name AS player, i.name AS item ' + join + ' ORDER BY c.id, i.id',
             [['Ana', 'Hat'], ['Ana', 'Coat'], ['Bo', 'Shoe'], ['Cam', 'Hat'], ['Cam', 'Coat']],
             ['player', 'item'])
        rows('SELECT c.name AS player, i.name ' + join +
             ' WHERE c.attack >= 10 AND (i.name != \'Coat\' OR c.id = 3) ORDER BY player DESC, i.id LIMIT 2 OFFSET 1',
             [['Cam', 'Coat'], ['Bo', 'Shoe']])
        rows('SELECT COUNT(*) ' + join, [['5']])
        rows('SELECT c.attack, COUNT(*) AS n, SUM(i.price) AS total ' + join +
             ' GROUP BY c.attack ORDER BY total DESC', [['10', '4', '40'], ['20', '1', '20']])
        rows('SELECT * ' + join + ' WHERE c.id=2', [['2', 'Bo', '20', '3', 'Shoe', '20']])
        rows('SELECT c.name, i.name ' + join + ' WHERE c.id=4', [])
        rows('SELECT characters.name, items.name FROM characters INNER JOIN items ON attack=price '
             'ORDER BY characters.id, items.id LIMIT 1 OFFSET 4', [['Cam', 'Coat']])
        rows('SELECT a.name, b.name FROM characters a JOIN characters b ON a.attack=b.attack '
             'WHERE a.id < b.id ORDER BY a.id, b.id', [['Ana', 'Cam']])
        rows('SELECT c.name FROM characters c JOIN items i ON c.name=i.name', [])
        rows('BEGIN;INSERT INTO items VALUES (\'Ana\',0);'
             'SELECT c.name,i.name FROM characters c JOIN items i ON c.name=i.name;', [['Ana', 'Ana']])
        # Check insert, update and delete overlays on both inputs, then rollback.
        _, results = run(path, 'BEGIN;UPDATE characters SET attack=40 WHERE id=1;'
                               'DELETE FROM items WHERE id=2;INSERT INTO items VALUES (\'New\',10);'
                               'INSERT INTO characters VALUES (\'Eli\',20);'
                               'SELECT c.name,i.name ' + join + ' ORDER BY c.name,i.name;ROLLBACK;'
                               'SELECT COUNT(*) ' + join + ';')
        assert results[-3]['rows'] == [['Ana', 'Bag'], ['Bo', 'Shoe'], ['Cam', 'Hat'],
                                      ['Cam', 'New'], ['Eli', 'Shoe']], results
        assert results[-1]['rows'] == [['5']], results

        errors = [
            ('SELECT id ' + join, 'ERR_UNKNOWN_COLUMN', 'Ambiguous'),
            ('SELECT c.id FROM characters c JOIN missing m ON c.id=m.id', 'ERR_UNKNOWN_TABLE', 'JOIN'),
            ('SELECT c.id FROM characters c JOIN items i ON c.attack=i.name', 'ERR_TYPE_MISMATCH', 'JOIN'),
            ('SELECT c.id FROM characters c JOIN items i ON c.attack=c.id', 'ERR_SYNTAX', 'each table'),
            ('SELECT a.id FROM characters a JOIN items a ON a.id=a.price', 'ERR_SYNTAX', 'distinct'),
            ('SELECT c.id ' + join + ' FOR UPDATE', 'ERR_SYNTAX', 'FOR UPDATE'),
            ('SELECT c.id FROM characters c LEFT JOIN items i ON c.id=i.id', 'ERR_SYNTAX', ''),
        ]
        for sql, code, message in errors:
            proc, results = run(path, sql, check=False)
            assert proc.returncode and results[-1]['error']['code'] == code, (sql, results)
            assert message in results[-1]['error']['message'], (sql, results)
        proc, results = run(path, 'SELECT COUNT(*) ' + join, env={'MDB_MAX_SORT_ROWS': '2'}, check=False)
        assert proc.returncode and results[-1]['error']['code'] == 'ERR_LIMIT_EXCEEDED', results
    print('PASS JOIN: integer/string keys, many-to-many, self-join, aggregates, pagination, overlays, errors and workspace limit')


if __name__ == '__main__':
    main()
