import arrow
from MediaPlaycounts import DailyIngest

def generate_dates():
    """
    Returns a list of Arrow date objects for each day since 1 January 2015 when
    these logs begin.
    """
    return arrow.Arrow.range('day', arrow.get('2015-01-01'), arrow.now())
    
if __name__ == '__main__':
    DailyIngest.run("../.my.cnf", db="s53189__mediaplaycounts_p", dates=generate_dates(), host="tools-db")
