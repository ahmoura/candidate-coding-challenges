import utils

## SQL Queries

def run_sql_queries(sc, query):

        sql_query = {   'q1': """ SELECT cast(extract(epoch from AVG(completeddate - startdate)) as integer) / (60 * 60) AS completeTimeOfACourse
                                FROM certificates """,
                        'q2': """ SELECT firstname, lastname, email, cast(extract(epoch from AVG(completeddate - startdate)) as integer) / (60 * 60) AS timeSpentInACourse
                                FROM certificates
                                GROUP BY  firstname, lastname, email""",
                        'q3': """ SELECT title, cast(extract(epoch from AVG(completeddate - startdate)) as integer) / (60 * 60) AS dateDifferenceInHours
                                FROM certificates
                                GROUP BY title """,
                        'q4': """SELECT * FROM (
                                SELECT *, cast(extract(epoch from AVG(completeddate - startdate)) as integer) / (60 * 60) AS dateDifferenceInHours
                                FROM certificates
                                GROUP BY title, firstname, lastname, email, completeddate, startdate) qs
                                WHERE dateDifferenceInHours = (SELECT MIN(aux) FROM (
                                                                                SELECT cast(extract(epoch from AVG(completeddate - startdate)) as integer) / (60 * 60) AS aux 
                                                                                FROM certificates
                                                                                GROUP BY title, firstname, lastname, email, completeddate, startdate) sqmin)
                                OR    dateDifferenceInHours = (SELECT MAX(aux) FROM (
                                                                                SELECT cast(extract(epoch from AVG(completeddate - startdate)) as integer) / (60 * 60) AS aux 
                                                                                FROM certificates
                                                                                GROUP BY title, firstname, lastname, email, completeddate, startdate) sqmax)""",
                        'q5': """SELECT firstname, lastname, email, COUNT(*)
                                FROM certificates
                                GROUP BY firstname, lastname, email"""
        }


        utils.read_sql_table(sc, "gold", sql_query[query]).show()