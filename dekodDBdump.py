"""
Dekoder databasedump fra mariadbpython.py
"""

import pickle
import json 
import pandas as pd
from datetime import datetime

def statusEndringssett( mariadbdump:dict, returner=False, detaljert=False, objektType=None   ): 
    """
    Printer status for endringssett på denne mariadumpen 

    ARGUMENTS: 
        mariadbdump - output fra mariadbpython.hentAltFraKontrakt

    KEYWORDS:
        returner=True for å få returnert dataframe med endringssett-tabellen 

        detaljert=True for å printe detaljert feilbeskrivelse fra SKRIV

        objekttype: Str, int eller list med int, objekttype ID

    RETURNS 
        None eller pandas Dataframe (keyword returner=True)
    """

    assert isinstance( mariadbdump, dict), f"Input må være dictionary med datadump fra mariadb"
    if 'nvdb_submission' not in mariadbdump: 
        print( f"Ingen endringsett i denne datadumpen!")
        return 
    
    assert isinstance( mariadbdump['nvdb_submission'], list ), f"Datatype for nvdb_submission skal være liste med dictionaries, fant {type(mariadbdump['nvdb_submission'])}"
    if len( mariadbdump['nvdb_submission']) == 0: 
        print( f"Tom liste i tabell med endringssett ('nvdb_submission')")
        return 
    assert isinstance( mariadbdump['nvdb_submission'][0], dict), f"Datatype for nvdb_submission skal være liste med dictionaries, fant {type(mariadbdump['nvdb_submission'][0])}"
    
    if objektType: 
        if isinstance( objektType, list) and len( objektType ) > 0: 
            objektType.sort()
            objektType = ','.join( [ str(x) for x in objektType ])
        elif isinstance( objektType, int): 
            objektType = str( objektType )
        elif isinstance( objektType, str ): 
            pass 
        else: 
            print( f"Kjente ikke igjen parameter objektType av type( {type(objektType)}) - må være heltall, str eller liste med heltall ")
            objektType = None 

    nvdb_submission = pd.DataFrame( mariadbdump['nvdb_submission'])
    nvdb_submission.sort_values( by='created', inplace=True )
    vilhakolonner = ['created', 'feature_type_ids', 'status', 'rejection_reason', 'owner', 'errors']
    if 'changeset_self_uri' in nvdb_submission.columns: 
        nvdb_submission['changesetID'] = nvdb_submission['changeset_self_uri'].apply( lambda x : x.split( '/')[-1] if x else '' )
        vilhakolonner.append( 'changesetID' )
    else: 
        print( f"Ingen kolonne 'changeset_self_uri', har vi ikke registrert endringssett på kontrakten?") 

    # Filtrerer
    if objektType: 
        nvdb_submission = nvdb_submission[ nvdb_submission['feature_type_ids'].str.contains( objektType) ]
        if len( nvdb_submission ) == 0: 
            print( f"ADVARSEL - Fikk null resultat når jeg filtrerte på objekttype {objektType}")

    kolonner = set( nvdb_submission.columns )
    utskriftkolonner = list( kolonner.intersection( set( vilhakolonner )) ) 
    if len( utskriftkolonner ) < len( vilhakolonner ): 
        print( f"Savner disse kolonnene i 'nvdb_submission' tabellen: { set( vilhakolonner ) - set( utskriftkolonner ) }")

    print( nvdb_submission[ utskriftkolonner ]  )

    if detaljert: 
        for ix, row in nvdb_submission.iterrows(): 
            if row['status'] == 'COMPLETED': 
                print( f"---- HURRA, skrev objekttyper {row['feature_type_ids']} {row['created']} {row['changesetID']} {row['status']} ")
            else: 
                print( f"\n===============  {row['status']} {row['rejection_reason']} ================\n")
                print( f"{row['created']} {row['owner']} {row['changesetID']} {row['status']} {row['rejection_reason']} ")
                if row['errors']: 
                    errors = row['errors'].split( '|')
                    print( f"\n")
                    for error in errors: 
                        print( f"\t{error}")
                        if len( error) > 120:
                            print( f"\n")
                else: 
                    print( f"Ingen detaljert feilmelding - tyder på at endringssett {row['changesetID']} ikke ble registret?")
                print( f"\n")

    if returner: 
        return nvdb_submission 

def feature2geojson( feature_id:str, mariadbdata:dict ): 
    """
    Komponerer geojson-feature for angitt feature_id i den datastrukturen vi har hentet fra mariadb 
    """

    kandidat = [ x for x in mariadbdata['feature2'] if feature_id ==  x['id'] ]
    assert len( kandidat ) < 2, f"Korrupte data - flere enn ett objekt i tabell feature2 har id='{feature_id}'"
    if len( kandidat ) < 1: 
        print( f"Fant ingen objekt med id='{feature_id}'")
        return None 
    
    feat = kandidat[0]
    
    # Henter geometri 
    geomkandidat = [ x for x in mariadbdata['feature_geometry'] if x['feature_id'] == feature_id ]
    assert len( geomkandidat ) == 1, f"Korrupte data - det finnes alltid EN og kun EN geometri for feature_id = '{feature_id}', jeg fant {len( geomkandidat)}"
    myGeojson = lagGeojsonGeometri( json.loads( geomkandidat[0]['geometry'] ) )

    myGeojson['properties']['tag']                  = feat['name']
    myGeojson['properties']['data_catalog_version'] = feat['data_catalog_version']
    myGeojson['properties']['type_id']              = feat['type_id']

    egenskaper = [ x for x in mariadbdata['feature_attribute2'] if x['feature_id'] == feature_id ]
    if len( egenskaper ) > 0: 
        myGeojson['properties']['attributes'] = { }
        for eg in egenskaper: 
            myGeojson['properties']['attributes'][eg['type_id']] = eg['value']

    # Kommentarer
    kommentarer = [ x['comment'] for x in mariadbdata['comment'] if x and x['object_id'] == feature_id ]
    kommentar_tekst = f"DF-databaseeksport {mariadbdata['eksportdato']}"
    if len( kommentarer ) > 0:
        kommentar_tekst += ' , kommentarer: ' + '; '.join( kommentarer )
        
    myGeojson['properties']['comment'] = kommentar_tekst


    return myGeojson

def lagGeojsonGeometri( dfgeom:dict ):
    """
    Går fra datafangst geometri-koding til geojson formulering 

    returnerer geojson-feature med geometri og kvalitetsparametre (properties.geometryAttributes), hvis de finnes

    MERK: Laget ut fra "fail-early" prinsipp, dvs vi tryner (kaster feil) hvis vi møter kompleksitet som vi ikke har møtt på før. 

    TODO: 
     - Støtte Multi-geometrityper (MultiLineString, MultiPoint )

    Dvs oversetter fra denne representasjonen (fra Datafangst-databasen)
        {
            "type": "POINT",
            "representationPoint": null,
            "shape": {
                "@class": "no.svv.nvdb.datafangst.domainmodel.geometry.shapes.Point",
                "position": {
                    "northing": 6586865.243002282,
                    "easting": -48327.84467091889,
                    "height": 6.885
                }
            },
            "srid": 25833,
            "heightRef": "NN2000",
            "properties": {
                "map": {
                    "ACCURACY": "5",
                    "CAPTURE_DATE": "2023-06-13",
                    "ACCURACY_HEIGHT": "5",
                    "VISIBILITY": "0"
                }
            },
            "length": 0.0,
            "operation": "WRITE"
        }
    
    til Geojson-representasjon
    
        {
            "type": "Feature",
            "geometry": {
                "type": "point",
                "coordinates": [
                    -48327.84467091889,
                    6586865.243002282,
                    6.885
                ]
            },
            "properties": {
                "geometryAttributes": {
                    "accuracy": "5",
                    "captureDate": "2023-06-13",
                    "accuracyHeight": "5",
                    "visibility": "0"
                }
            }
        }
        
    """

    def lagPunkt( point:dict ):
        myList = [ point['easting'], point['northing']]
        if 'height' in point and point['height'] != 'nan': 
            myList.append( point['height'])
        return myList 
    
    def lagPunktSerie( punktListe:list ):
        myList = []
        for point in punktListe: 
            myList.append( lagPunkt( point ))

        return myList

    if dfgeom['type'] == 'POINT': 
        geom = { "type" : "point", 
                "coordinates" : lagPunkt( dfgeom['shape']['position'] ) }
    elif dfgeom['type'] == 'LINE': 
        geom = { "type" : "LineString",
                "coordinates" : lagPunktSerie( dfgeom['shape']['positions'] ) }       

    elif dfgeom['type'] == 'POLYGON': 
        geom = { "type" : "Polygon", 
                "coordinates" : [ lagPunktSerie( dfgeom['shape']['exteriorRing']['positions'] ) ] }
        
        if 'interiorRings' in dfgeom['shape'] and len( dfgeom['shape']['interiorRings'] ) > 0: 
            raise NotImplementedError( f"Sorry, har ikke implementert 'interiorRings' for polygon ennå")

    else: 
        raise NotImplementedError( f"Sorry, har ikke rukket å implementere dekoding av geometritypen {dfgeom['type']} ennå")
    
    assert dfgeom['srid'] == 25833 or dfgeom['srid'] == 5973, f"Ikke implementert støtte for SRID={dfgeom['srid']} ennå"
    assert dfgeom['heightRef'] == 'NN2000', f"Ikke implementert støtte for høydereferanse={dfgeom['heightRef']} ennå"

    GJfeature = { "type" : "Feature", "geometry" : geom, "properties" : { } }

    # Taggene for geometrikvalitet bør nok oversettes, 
    # eks CAPTURE_DATE => captureDate, ref https://apiskriv.vegdata.no/datafangst/datafangst-api#format 
    def camel(snake_str):
        """
        Intern funksjon som oversetter CAPTURE_DATE => captureDate 
        https://stackoverflow.com/questions/19053707/converting-snake-case-to-lower-camel-case-lowercamelcase
        """
        first, *others = snake_str.split('_')
        return ''.join([first.lower(), *map(str.title, others)])

    if 'map' in dfgeom['properties']: 
        GJfeature['properties']["geometryAttributes"] = { } 
        for geomATTR in dfgeom['properties']['map'].keys(): 
            nyGeomATTR = camel( geomATTR )
            GJfeature['properties']["geometryAttributes"][nyGeomATTR] = dfgeom['properties']['map'][geomATTR]

    return GJfeature