import mysql.connector
import pandas as pd 
import json 


def lagCursor( secretsfile='secrets.json', database=None ): 
    """
    Leser oppkoblingsdetaljer, returnerer connnection- og cursor objekt

    NB! Det er god folkeskikk å lukke cursor og oppkobling etterpå
    """

    with open( secretsfile ) as f: 
        secrets = json.load( f )

    # Kan bruke en annen database, f.eks for å se på lokal backup i annen database
    if database: 
        secrets['database'] = database
      
    conn = mysql.connector.connect( user=secrets['user'], 
                                   password=secrets['password'], 
                                   host=secrets['host'], 
                                   database=secrets['database'] )


    cursor = conn.cursor()

    return (conn, cursor)    

def hentFraTabell( tabellNavn:str, cursor, modifikator='LIMIT 10'):
    """
    Henter data fra angitt tabell. Modifikator kan f.eks være WHERE  - statement eller noe sånt. 
    """

    skjema = hentSkjema( tabellNavn, cursor )

    query = f"SELECT * from {tabellNavn} {modifikator}"
    cursor.execute( query)
    data = []
    for row in cursor: 
        myRow = {}
        for ii, col in enumerate(row): 
            myRow[ skjema['FieldNames'][ii] ] = col
        data.append( myRow )

    return data 


def hentSkjema( tabell:str, cursor ):
    """
    Lager skjema for databasetabell 
    """

    query = (f"show full columns from {tabell}")
    # Som ser slik ut: 
    # +-------+--------------+-------------------+------+-----+---------+-------+---------------------------------+---------+
    # | Field | Type         | Collation         | Null | Key | Default | Extra | Privileges                      | Comment |
    # +-------+--------------+-------------------+------+-----+---------+-------+---------------------------------+---------+
    # | id    | char(36)     | ascii_general_ci  | NO   | PRI | NULL    |       | select,insert,update,references |         |
    # | name  | varchar(100) | utf8mb4_danish_ci | NO   | UNI | NULL    |       | select,insert,update,references |         |
    # +-------+--------------+-------------------+------+-----+---------+-------+---------------------------------+---------+
    skjema = { 'FieldNames' : [] }
    cursor.execute( query)
    for row in cursor: 
        skjema['FieldNames'].append( row[0])
        skjema[ row[0] ] = { 'Field'        : row[0], 
                            'Type'          : row[1],
                            'Collation'     : row[2], 
                            'Null'          : row[3],
                            'Key'           : row[4],
                            'Default'       : row[5],
                            'Extra'         : row[6],
                            'Privileges'    : row[7],
                            'Comment'       : row[8]
                            }



    return skjema


