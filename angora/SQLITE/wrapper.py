##encoding=utf8

def iterC(cursor, arraysize = 1000):
    "An iterator that uses fetchmany to keep memory usage lower"
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result
            
def prt_all(cursor):
    """equivalent to:
    for row in c.fetchall():
        print(row)
    """
    counter = 0
    for row in iterC(cursor):
        print(row)
        counter += 1
    print("Found %s records" % counter)