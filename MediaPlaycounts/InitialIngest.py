import arrow

def generate_dates():
    """
    Returns a list of Arrow date objects for each day since 1 January 2015 when
    these logs begin.
    """
    return arrow.Arrow.range('day', arrow.get('2015-01-01'), arrow.now())

#def run(read_default_file, db="mediaplaycounts", dates=generate_dates(),
#        host="localhost", port=3306):
    
