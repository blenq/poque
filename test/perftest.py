from datetime import datetime

from psycopg2 import connect as psyco_connect
from poque import connect as poque_connect
# from poque.ctypes import connect as poque_connect


def test_many_queries_int(cr):
    queries = ["SELECT {}".format(i) for i in range(1000)]
    for i in range(100):
        for query in queries:
            cr.execute(query)
            cr.fetchone()


def test_many_queries_text(cr):
    queries = ["SELECT 'text{}'".format(i) for i in range(1000)]
    for i in range(100):
        for query in queries:
            cr.execute(query)
            cr.fetchone()


def test_many_queries_int_text_param(cr):
    if hasattr(cr, "cast"):
        query = "SELECT %s, %s"
    else:
        query = "SELECT $1, $2"
    queries = [(query, (i, "test{}".format(i))) for i in range(1000)]
    ret = datetime.now()
    for i in range(50):
        for query, params in queries:
            cr.execute(query, params)
            cr.fetchone()
    return ret


def test_large_result(cr):
    cr.execute("CREATE TEMPORARY TABLE yo (num int, string text)")
    for i in range(100):
        cr.execute("INSERT INTO yo VALUES ({}, 'text{}')".format(i, i))
    ret = datetime.now()
    for i in range(2):
        cr.execute("SELECT * FROM yo t, yo a, yo f")
        while True:
            if cr.fetchone() is None:
                break
    return ret


def tester(psyco_cr, poque_cr, func):
    for name, cr in (("psyco", psyco_cr), ("poque", poque_cr)):
        dt = datetime.now()
        ret = func(cr)
        if ret is not None:
            dt = ret
        td = datetime.now() - dt
        print("{}: {}".format(name, td))


def main():
    psyco_conn = psyco_connect(dbname="poque_test")
    poque_conn = poque_connect(dbname="poque_test")

    psyco_cr = psyco_conn.cursor()
    poque_cr = poque_conn.cursor()
    tester(psyco_cr, poque_cr, test_many_queries_int_text_param)
    tester(psyco_cr, poque_cr, test_large_result)
    tester(psyco_cr, poque_cr, test_many_queries_int)
    tester(psyco_cr, poque_cr, test_many_queries_text)


if __name__ == '__main__':
    main()
