import petl as etl
table1 = [['foo'],
           ['a', 1, True],
            ['b', 2, False]]
table2 = etl.extendheader(table1, ['bar', 'baz'])
print(table2)