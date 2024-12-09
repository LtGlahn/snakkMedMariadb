"""
Wrapper for å hente ut data fra datafangst databasen 

NB! Krever versjon 8 av mysql-connector. Versjon 9 tillater ikke lenger innlogging med brukernavn og passord. 
"""


import mysql.connector
import pandas as pd 
import pickle
import json 
import subprocess
import os 
from datetime import datetime

import dekodDBdump

def lagCursor(secretsfile='secrets.json', database=None, AWS=True):
    """
    Leser oppkoblingsdetaljer, returnerer connnection- og cursor objekt

    NB! Det er god folkeskikk å lukke cursor og oppkobling etterpå

    ARGUMENTS
        N/A

    KEYWORDS
        secretsfile='secrets.json' JSON file with credentials packed in a dictionary

        database=None eller tekst=navnet på en annen database enn det som står i secretsfile

        AWS=True Sett lik False om du kjører mot gammelt oppsett med database lokalt. Oppklobling mot AWS 
                krever gyldig sertifikatfil angitt i secretfsfile 

    RETURNS 
        (conn, cursor) =  mysql.connector "connection" og "cursor" objekt 
    """
    try:
        with open(secretsfile) as f:
            secrets = json.load(f)

        # Kan bruke en annen database, f.eks for å se på lokal backup i annen database
        if database:
            secrets['database'] = database

        if AWS: 
            conn = mysql.connector.connect(
                user=secrets['user'],
                password=secrets['password'],
                host=secrets['host'],
                database=secrets['database'],
                port=secrets['port'],
                ssl_ca=secrets.get('ssl_ca')
            )
        else: 
            conn = mysql.connector.connect(
                user=secrets['user'],
                password=secrets['password'],
                host=secrets['host'],
                database=secrets['database'],
                port=secrets['port']
            )
        cursor = conn.cursor(dictionary=False)
        return conn, cursor
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        raise


def fiks2Dmetadata( kontraktId, dryrun=True, **kwargs ): 
    """
    Leser geometri for objektene i kontrakt og sjekker og retter opp metadata de 2D objektene som har 3D metadata

    ARGUMENTS
        kontraktId:str, ID på datafangst kontrakten 

    KEYWORDS
        alle nøkkelord sendes videre til funksjonen lagCursor 

    RETURNS 
        (connection, cursor) - for å kunne interagere og gjøre evt rollback 
    """

    conn, cursor = lagCursor( **kwargs )

    try: 
        # feature2 = hentFraTabell( 'feature2', cursor, modifikator=f"where project_id  = '{kontraktId}' AND nvdb_id is not NULL", databegrensning=False )
        feature2 = hentFraTabell( 'feature2', cursor, modifikator=f"where project_id  = '{kontraktId}'", databegrensning=False )
        featureID = [ x['id'] for x in feature2 ]
        if len( featureID ) == 0: 
            print( f"Fant ingen NVDB objekt i kontrakt {kontraktId} ???")
            conn.close()
            return  (conn, cursor)
        temp = f"where feature_id is in ({ ','.join( featureID ) })"
        print( f"SQL setning: \n{temp}\n")
        geometri = hentFraTabell( 'feature_geometry', cursor, modifikator=temp, databegrensning=False )

    except Exception as e: 
        print( f"Datauthenting feilet: {e}")
        conn.close()
        return (conn, cursor)


    sql_setninger = dekodDBdump.fiks2Dgeom2sql( geometri )

    if len( sql_setninger ) == 0:
        print( f"Fant ingen geometrier med feil på metadata på kontrant {kontraktId}")
        conn.close( )
        return (conn, cursor )
    
    print( f"Fant {len( sql_setninger)} objekter med 3D metadata på 2D geometri")

    if dryrun: 
        print( "DRYRU - her er SQL setningene")
        for enSQL in sql_setninger: 
            print( enSQL)

        conn.close()
        return (conn, cursor)

    try: 
        cursor.execute( "BEGIN TRANSACTION;")

        for enSQL in sql_setninger: 
            print( enSQL )
            cursor.execute( enSQL )

        conn.commit( )

    except Exception as e: 
        print( f"Feilmelding på SQL update: {e}, ruller tilbake")
        conn.rollback()

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


def kontraktdump2excel( kontraktdump:dict, filnavn:str  ):
    """
    Lagrer output fra hentAltFraKontrakt til excel-fil 

    Vi ignorerer tomme tabeller 
    """
    arknavn = []
    dataFrames = []

    assert isinstance( kontraktdump, dict), f"Input data må være dictionary med lister"

    for tabellNavn in kontraktdump.keys(): 
        if isinstance( kontraktdump[tabellNavn], list): 
            if len( kontraktdump[tabellNavn]) > 0: 
                myDf = pd.DataFrame( kontraktdump[tabellNavn] )
                arknavn.append( tabellNavn)
                dataFrames.append( myDf )
            else: 
                print( f"Ignorerer tom tabell {tabellNavn}")
        elif tabellNavn == 'eksportdato': 
            pass 
        else: 
            print( f"Datasett {tabellNavn} er ikke en liste")

    skrivexcel( filnavn, dataFrames, sheet_nameListe=arknavn )

def hentAltOmObjekt( ):
    """
    IKKE IMPLEMENTERT
    """
    pass 

def hentAltFraKontrakt( kontraktId, database='datafangst', excelfil=None, picklefil=None, sendTilLangbein=False, **kwargs ): 
    """
    Henter alle data tilknyttet en kontrakt 

    Returnerer dictionary med en key per tabell, som hver har (potensielt tom) liste med dictionaries

    ARGUMENTS
        kontraktId:str, ID på datafangst kontrakten 

    KEYWORDS
        excelfil=None eller filnavn på excelfil som du dumpler data til 

        picklefil=None eller filnavn på pickle datadump

        sendTilLangbein=False. Sett til True for å få scp-kommando for å overføre data til FoU-server Langbein 

        Alle andre nøkkelord sendes til funksjonen lagCursor

    RETURNS
        dictionary med en liste per tabell pluss tidsstempel 'eksportdato' 
    """

    conn, cursor = lagCursor( **kwargs )

    resultat = {
            'eksportdato'           : datetime.now(),
            'feature_association2'  : [],  
            'feature_attribute2'    : [], 
            'feature_geometry'      : [], 
            'feature_locational2'   : [],
            'feature_locks'         : [],
            'file'                  : [],
            }

    resultat['project']                 = hentFraTabell( 'project',             cursor, modifikator=f"where id          = '{kontraktId}'")
    resultat['comment']                 = hentFraTabell( 'comment',             cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['contract_change']         = hentFraTabell( 'contract_change',     cursor, modifikator=f"where contract_id = '{kontraktId}'")
    resultat['contract_visit']          = hentFraTabell( 'contract_visit',      cursor, modifikator=f"where contract_id = '{kontraktId}'")
    resultat['event']                   = hentFraTabell( 'event',               cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['feature2']                = hentFraTabell( 'feature2',            cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['nvdb_submission']         = hentFraTabell( 'nvdb_submission',     cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['project_locks']           = hentFraTabell( 'project_locks',       cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['project_map_comment']     = hentFraTabell( 'project_map_comment', cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['project_milestone']       = hentFraTabell( 'project_milestone',   cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['validation_issue2']       = hentFraTabell( 'validation_issue2',   cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['file']                    = hentFraTabell( 'file',                cursor, modifikator=f"where project_id  = '{kontraktId}'")
    resultat['user_role']               = hentFraTabell( 'user_role',           cursor, modifikator=f"where contract_id = '{kontraktId}'")


    for feat in resultat['feature2']:
        relasjoner = hentFraTabell( 'feature_association2', cursor, modifikator=f"where parent_feature_id = '{feat['id']}' or child_feature_id = '{feat['id']}' ")
        resultat['feature_association2'].extend( relasjoner )

        egenskaper = hentFraTabell( 'feature_attribute2', cursor, modifikator=f"where feature_id = '{feat['id']}'")
        resultat['feature_attribute2'].extend( egenskaper )

        geometri = hentFraTabell( 'feature_geometry', cursor, modifikator=f"where feature_id = '{feat['id']}'")
        resultat['feature_geometry'].extend( geometri )

        stedfesting = hentFraTabell( 'feature_locational2', cursor, modifikator=f"where feature_id = '{feat['id']}'")
        resultat['feature_locational2'].extend( stedfesting )

        lock = hentFraTabell( 'feature_locks', cursor, modifikator=f"where feature_id = '{feat['id']}'")
        resultat['feature_locks'].extend( lock )

    cursor.close()
    conn.close()

    if excelfil: 
        kontraktdump2excel( resultat, excelfil)
        if sendTilLangbein: 
            print( f"!scp -P 1932 {excelfil} jajens@its.npra.io:/var/www/html/datafangstdump")

    if picklefil: 
        with open( picklefil, 'wb') as f:
            pickle.dump( resultat, f )
        
        if sendTilLangbein: 
            print( f"!scp -P 1932 {picklefil} jajens@its.npra.io:/var/www/html/datafangstdump")

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


def skrivexcel( filnavn, dataFrameListe, sheet_nameListe=[], indexListe=[] ):
    """
    Skriver liste med dataFrame til excel, med kolonnebredde=lengste element i header eller datainnhold

    ARGUMENTS
        filnavn : Navn på excel-fil 

        dataFrameListe : Liste med dataframe, eller en enkelt dataFrame / geodataframe 

    KEYWORDS
        sheet_nameListe : [] Liste med navn på fanene i exel-arket. Hvis tom liste brukes Fane1, Fane2...

        indexListe : [] Angir om index skal med som første kolonne(r), liste med True eller False. Default: Uten index. 

        slettgeometri : True . Sletter geometrikolonner 
    """

    # Håndterer en enkelt dataframe => putter i liste med ett element

    if not isinstance( dataFrameListe, list ): 
        dataFrameListe = [ dataFrameListe ]

    writer = pd.ExcelWriter( filnavn, engine='xlsxwriter')


    for (idx, endf ) in enumerate( dataFrameListe): 

        # Sikrer at vi ikke har sideeffekter på orginal dataframe
        mydf = endf.copy()

        # Navn på blad (ark, sheet_name) i excel-fila
        if sheet_nameListe and isinstance( sheet_nameListe, list) and idx+1 <= len( sheet_nameListe): 
            arknavn = sheet_nameListe[idx]
        else: 
            arknavn = 'Ark' + str( idx+1 )

        # Skal vi ha med indeks? 
        if indexListe and isinstance( indexListe, list) and len( indexListe) <= idx+1: 
            brukindex = indexListe[idx]
        else: 
            brukindex = False 

        mydf.to_excel(writer, sheet_name=arknavn, index=brukindex)


        # Auto-adjust columns' width. 
        # Fra https://towardsdatascience.com/how-to-auto-adjust-the-width-of-excel-columns-with-pandas-excelwriter-60cee36e175e
        for column in mydf:
            column_width = max(mydf[column].astype(str).map(len).max(), len(column)) + 3
            col_idx = mydf.columns.get_loc(column)
            writer.sheets[arknavn].set_column(col_idx, col_idx, column_width)

    writer.close( )
    print( f"skrev {len( dataFrameListe )} faner til {filnavn} ")