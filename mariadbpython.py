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

def hentFraTabell( tabellNavn:str, cursor, modifikator='LIMIT 10', databegrensning=True ):
    """
    Henter data fra angitt tabell. Modifikator kan f.eks være WHERE  - statement eller noe sånt. 
    """

    skjema = hentSkjema( tabellNavn, cursor )

    if databegrensning and not 'LIMIT' in modifikator.upper(): 
        modifikator += ' LIMIT 5000'

    query = f"SELECT * from {tabellNavn} {modifikator}"
    cursor.execute( query)
    data = []
    for row in cursor: 
        myRow = {}
        for ii, col in enumerate(row): 
            myRow[ skjema['FieldNames'][ii] ] = col
        data.append( myRow )

    return data 

def hentAltFraKontrakt( kontraktId, cursor ): 
    """
    Henter alle data tilknyttet en kontrakt 
    """

    resultat = {
            'feature_association2' : [],  
            'feature_attribute2' : [], 
            'feature_geometry' : [], 
            'feature_locational2' : [],
            'feature_locks' : [],
            'file' : [],
            }

    resultat['project'] = hentFraTabell( 'project', cursor, modifikator=f"where project_id = {kontraktId} ")
    resultat['comment'] = hentFraTabell( 'comment', cursor, modifikator=f"where project_id = {kontraktId} ")
    resultat['contract_change'] = hentFraTabell( 'contract_change', cursor, modifikator=f"where contract_id = {kontraktId} ")
    resultat['contract_visit'] = hentFraTabell( 'contract_visit', cursor, modifikator=f"where contract_id = {kontraktId} ")
    resultat['event'] = hentFraTabell( 'event', cursor, modifikator=f"where project_id = {kontraktId} ")
    resultat['feature2'] = hentFraTabell( 'feature2', cursor, modifikator=f"where project_id = {kontraktId} ")
    resultat['nvdb_submission'] = hentFraTabell( 'nvdb_submission', cursor, modifikator=f"where project_id = {kontraktId}")
    resultat['project_locks'] = hentFraTabell( 'project_locks', cursor, modifikator=f"where project_id = {kontraktId}")
    resultat['project_map_comment'] = hentFraTabell( 'project_map_comment', cursor, modifikator=f"where project_id = {kontraktId}")
    resultat['project_milestone'] = hentFraTabell( 'project_milestone', cursor, modifikator=f"where project_id = {kontraktId}")
    resultat['validation_issue2'] = hentFraTabell( 'validation_issue2', cursor, modifikator=f"where project_id = {kontraktId}")

    for feat in resultat['feature2']:
        relasjoner = hentFraTabell( 'feature_association2', cursor, modifikator=f"where parent_feature_id = {feat['id']} or child_feature_id = {feat['id']} ")
        resultat['feature_association2'].extend( relasjoner )

        egenskaper = hentFraTabell( 'feature_attribute2', cursor, modifikator=f"where feature_id = {feat['id']}")
        resultat['feature_attribute2'].extend( egenskaper )

        geometri = hentFraTabell( 'feature_geometry', cursor, modifikator=f"where feature_id = {feat['id']}")
        resultat['feature_geometry'].extend( geometri )

        stedfesting = hentFraTabell( 'feature_locational2', cursor, modifikator=f"where feature_id = {feat['id']}")
        resultat['feature_locational2'].extend( stedfesting )

        lock = hentFraTabell( 'feature_locks', cursor, modifikator=f"where feature_id = {feat['id']}")
        resultat['feature_locks'].extend( lock )

        filer = hentFraTabell( 'file', cursor, modifikator=f"where feature_id = {feat['id']}")
        resultat['file'].extend( filer )


    return resultat

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


