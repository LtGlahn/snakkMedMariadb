"""
Dekoder databasedump fra mariadbpython.py
"""

import pickle
import json 
import pandas as pd
from datetime import datetime
from copy import deepcopy



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
            assert all( isinstance( x, (int, str) ) for x in objektType ), f"objektType må være heltall, str eller liste med heltall"
            objektType.sort()
            objektType = ','.join( [ str(x) for x in objektType ])
        elif isinstance( objektType, int): 
            objektType = str( objektType )
        elif isinstance( objektType, str ): 
            pass 
        else: 
            print( f"Kjente ikke igjen parameter objektType av type( {type(objektType)}) - må være heltall eller liste med heltall IGNORERER PARAMETER")
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
    

def QAskriveoperasjoner( operasjon ):
    """
    Kvalitetssikrer at operasjon er en eller flere av ['CREATE', 'CORRECT', 'UPDATE', 'CLOSE']
    evt kan man også bruke de norske begrepene til NVDB APISKRIV 
    'registrer', 'oppdater', 'delvisOppdater', 'korriger', 'delvisKorriger', 'lukk'

    Merk at 'fjern' ikke anerkjennes som en lovlig operasjon for Datafangst endringssett (i denne konteksten), 
    og vil bli ignorert

    ARGUMENTS
        operasjon : list ELLER str ELLER None 

    RETURNS
        DFoperasjon : list, liste med null, 1 eller flere av verdiene ['CREATE', 'CORRECT', 'UPDATE', 'CLOSE']
    """

    DFoperasjon = []
    if operasjon is None: 
        return DFoperasjon 

    # Skriveoperasjoner
    muligeOperasjoner = ['CREATE', 'CORRECT', 'UPDATE', 'CLOSE']
    norskeOperasjoner = {'REGISTRER'        :  'CREATE', 
                         'OPPRETT'          : 'CREATE',
                         'OPPDATER'         : 'UPDATE', 
                         'DELVISOPPDATER'   : 'UPDATE', 
                         'KORRIGER'         : 'CORRECT', 
                         'DELVISKORRIGER'   : 'CORRECT', 
                         'SLETT'            : 'CLOSE', 
                         'LUKK'             : 'CLOSE'    }
  
    # Gjør tekst om til python-liste med tekst.
    # Merk at syntaksen operasjon='registrer,UPDATE' er helt grei syntaks 
    if isinstance( operasjon, str):
        if ',' in operasjon: 
            operasjonListe = operasjon.split(',') 
        else:  
            operasjonListe = [ operasjon ]

    assert isinstance( operasjonListe, list), f"Parameter operasjon må være tekststreng eller liste med operasjoner, ikke {type(operasjon)} "
    assert all( isinstance( x, str) for x in operasjonListe), f"Operasjon må være tekst, ikke {[type( x) for x in operasjonListe]}"
    operasjonListe = [ x.upper() for x in operasjonListe ]
    for enOperasjon in operasjonListe: 
        if enOperasjon in norskeOperasjoner: 
            print( f"Oversetter norsk operasjon til internt DF1.0 - vokabular: {enOperasjon} => {norskeOperasjoner[enOperasjon]}")
            DFoperasjon.append(  norskeOperasjoner[enOperasjon] ) 
        elif enOperasjon in muligeOperasjoner: 
            DFoperasjon.append( enOperasjon )
        else: 
            print( f"Gjenkjenner ikke og vil igonorere denne operasjonstypen: {enOperasjon}")
    if len( DFoperasjon ) == 0: 
        print( f"Ingen lovlige verdier i parameter operasjon={operasjon}, ignorerer")

    return DFoperasjon   

    
def eksport2geojson( mariadbdump:dict, filename=None, objektType=None, alias=None, name=None, operasjon=None  ): 
    """
    Eksporterer alle eller noen features til geojson featurecollection

    ARGUMENTS: 
        mariadbdump - output fra mariadbpython.hentAltFraKontrakt

    KEYWORDS: 
        filename : None ELLER str, vil lagre til angitt filnavn istedet for å returnerer geojson dictionary

        objektType : None ELLER Int ELLER list, vil kun returnere angitt objekttyp(er)

        alias : None ELLER str, vil filtrere på de objektene der alias-feltet inneholder søkestrengen
        
        name  : None ELLER str, vil filtrere på de objektene der name-feltet inneholder søkestrengen

        operasjon : None ELLER str = en av ['CREATE', 'CORRECT', 'UPDATE', 'CLOSE'], eller en liste med en eller fler av dem
                        eventuelt kan man også bruke de norske begrepene som er mulige i NVDB SKRIVEAPI 
                        se dokumentasjon på dekodDBdump.def QAskriveoperasjoner( operasjon ):

    RETURNS 
        dictionary ELLER NONE, avhenger av om filename er angitt eller ei. dictionary er en geojson featureCollection
    """

    assert isinstance( mariadbdump, dict), f"Input må være dictionary med datadump fra mariadb"
    assert 'eksportdato' in mariadbdump, f"Kjenner ikke igjen dictionary som datadump fra mariadb"
    if 'feature2' not in mariadbdump: 
        print( f"Ingen feature2 - tabell i denne datadumpen!")
        return     
    
    FeatCollection = {
                        "type" : "FeatureCollection",
                        "name" : f"DFdatadump {str(mariadbdump['eksportdato'])}",
                        "crs" : {
                            "type" : "name",
                            "properties" : {
                                "name" : "EPSG:5973"
                                    }
                        },
                        "features" : [ ]
                    }

    if objektType: 
        # objektType blir en liste med ett eller flere medlemmer
        assert isinstance( objektType, (int, list)), f"objektType må være heltall eller liste med heltall"
        if isinstance( objektType, list): 
            assert all( isinstance( x, int) for x in objektType), f"ObjektType som liste må kun inneholde heltall"
        elif isinstance( objektType, int): 
            objektType = [ objektType ]
        else: 
            print( f"Kjente ikke igjen objektType - parameter??? Type {type(objektType)}, avbryter")
            return 
        
        # Føyer filter til navnet på featureCollection
        FeatCollection["name"] += f" objektType={','.join( [ str(x) for x in objektType] )}"

    if alias: 
        assert isinstance( alias, str), f"Parameter alias må være tekststreng"
        FeatCollection["name"] += f" alias={alias}"

    if name: 
        assert isinstance( name, str), f"Parameter name må være tekststreng"
        FeatCollection["name"] += f" name={name}"

    
    DFoperasjon = QAskriveoperasjoner( operasjon )


    for feat in mariadbdump['feature2']: 

        # Må sjekke de ulike filtrene
        godkjent = True 
        if objektType and feat['type_id'] not in objektType: 
            godkjent = False 

        if name and name.lower() not in feat['name'].lower(): 
            godkjent = False 

        if alias and alias.lower() not in feat['alias'].lower(): 
            godkjent = False 

        if len( DFoperasjon ) > 0: 
            if feat['operation'] not in DFoperasjon: 
                godkjent = False 

        if godkjent: 
            gj = feature2geojson( feat['id'], mariadbdump )
            FeatCollection['features'].append( gj )

    if len( FeatCollection['features']) == 0: 
        print( f"ADVARSEL - fant ingen features med angitte filtre. {FeatCollection['name']}")

    if filename: 
        assert isinstance( filename, str ), f"Argument filename må være tekst"
        with open( filename, 'w') as f: 
            json.dump( FeatCollection, f, ensure_ascii=False, indent=4 )
            print( f"Skrev geojson med {len( FeatCollection['features'])} medlemmer til fil {filename}")
    else: 
        return FeatCollection 

def feature2geojson( feature_id:str, mariadbdump:dict ): 
    """
    Komponerer geojson-feature for angitt feature_id i den datastrukturen vi har hentet fra mariadb 
    """

    kandidat = [ x for x in mariadbdump['feature2'] if feature_id ==  x['id'] ]
    assert len( kandidat ) < 2, f"Korrupte data - flere enn ett objekt i tabell feature2 har id='{feature_id}'"
    if len( kandidat ) < 1: 
        print( f"Fant ingen objekt med id='{feature_id}'")
        return None 
    
    feat = kandidat[0]
    
    # Henter geometri 
    geomkandidat = [ x for x in mariadbdump['feature_geometry'] if x['feature_id'] == feature_id ]
    assert len( geomkandidat ) == 1, f"Korrupte data - det finnes alltid EN og kun EN geometri for feature_id = '{feature_id}', jeg fant {len( geomkandidat)}"
    myGeojson = lagGeojsonGeometri( json.loads( geomkandidat[0]['geometry'] ) )

    myGeojson['properties']['tag']                  = feat['name']
    myGeojson['properties']['data_catalog_version'] = feat['data_catalog_version']
    myGeojson['properties']['type_id']              = feat['type_id']

    egenskaper = [ x for x in mariadbdump['feature_attribute2'] if x['feature_id'] == feature_id ]
    if len( egenskaper ) > 0: 
        myGeojson['properties']['attributes'] = { }
        for eg in egenskaper: 
            myGeojson['properties']['attributes'][eg['type_id']] = eg['value']

    # Kommentarer
    kommentarer = [ x['comment'] for x in mariadbdump['comment'] if x and x['object_id'] == feature_id ]
    kommentar_tekst = f"DF-databaseeksport {mariadbdump['eksportdato']}"
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


def endringsettVegobjekter( endringssett:dict ):
    """
    Leser vegobjektene i endringssett

    Returnerer dictionary med dataframes, en oppføring per operasjon

    ARGUMENTS
        endringssett : dict, endringssett fra NVDB api SKRIV

    KEYWORDS
        N/A
    
    RETURNS
        dictionary med én tabell med vegobjekter per skriveoperasjon i endringssettet. 
        Hver av tabellene er en pandas DataFrame. 
    """

    returdata = { 'endringssett' : endringssett }

    for operasjon in ['registrer', 'delvisOppdater', 'oppdater', 'korriger', 'delvisKorriger', 'lukk', 'fjern']: 

        if operasjon in endringssett: 
            returdata[operasjon] = pd.DataFrame( endringssett[operasjon]['vegobjekter'] )

    return returdata

def fjernHoydeMetadataFra2Dgeom( geomobj:dict ): 
    """
    Retter opp i metadata høyde for 2D geometrier

    Eksempel på 2D geometri (height='nan') og der nøyaktighet høyde (ACCURACY_HEIGHT) har verdien 1, som en fornuftig verdi for 3D, 
    men bare tøys og ikke tillatt for 2D geometri

    {'type': 'POINT',
  'representationPoint': None,
  'shape': {'@class': 'no.svv.nvdb.datafangst.domainmodel.geometry.shapes.Point',
   'position': {'northing': 6456810.56, 'easting': 56268.68, 'height': 'NaN'}},  <<<<---- 'NaN' for høydekoordinat == 2D geometri
  'srid': 5973,
  'heightRef': 'NN2000',
  'properties': {'map': {'ACCURACY': '1',
    'ACCURACY_HEIGHT': '1',                         <<<<<<<---- FEIL, bare tøys for 2D koordinat! 
    'MEASUREMENT_METHOD_HEIGHT': '-1',
    'VISIBILITY': '0',
    'MEASUREMENT_METHOD': '0'}},
  'length': 0.0,
  'operation': 'READ'}

    ARGUMENTS
        geomobj:dict, et objekt fra Datafangst tabellen feature_geometry 

    KEYWORDS 
        N/A

    RETURNS
        NONE eller dict. Returnerer gyldig 2D geometri der vi har fjernet metadata for høyde
    """

    newgeomobj = deepcopy( geomobj ) # Unngå snåle bieffekter av at vi endrer på ting utafor scope til funksjonen
    geom = json.loads( geomobj['geometry'])
    fiks2Dgeom = False 

    # Hvis det er 2D så har vi teksten 'nan' på height-koordinaten. 3D så er det et flyttall
    if geom['type'] == 'POINT': 
        if isinstance( geom['shape']['position']['height'], float): 
            return None
    elif geom['type'] == 'LINE': 
        if isinstance( geom['shape']['positions'][0]['height'], float):
            return None 
    else: 
        print( f"IKKE IMPLEMENTERT geometritype= {geom['type']}, hopper over men her er datadump\n")
        print( json.dumps( geom, indent=4 ))
        return None 
        # raise NotImplemented( f"Har ikke implementert støtte for geometritype {geom['type']}")
    
    # HVis vi når dette punktet så har vi 2D geometri, vi sjekker metadata
    if 'ACCURACY_HEIGHT' in geom['properties']['map']: 
        fiks2Dgeom = True 
        junk = geom['properties']['map'].pop( 'ACCURACY_HEIGHT')

    if 'MEASUREMENT_METHOD_HEIGHT' in geom['properties']['map']: 
        fiks2Dgeom = True 
        junk = geom['properties']['map'].pop( 'MEASUREMENT_METHOD_HEIGHT')

    if 'HEIGHTREF' in geom['properties']['map']: 
        fiks2Dgeom = True 
        junk = geom['properties']['map'].pop( 'HEIGHTREF' )

    if 'heightReference' in geom and geom['heightReference'] != None: 
        fiks2Dgeom = True 
        junk = geom.pop( 'heightReference' )

    if fiks2Dgeom: 
        newgeomobj['geometry'] = json.dumps( geom )
        return newgeomobj 


def fiks2Dgeom2sql( feature_geometry:list ): 
    """
    Returnerer liste med SQL setninger for de objektene som har ugyldige 3D metadata for 2D geometrier 
    """

    output = []
    for ii, geomobj in enumerate( feature_geometry): 
        fiksa = fjernHoydeMetadataFra2Dgeom( geomobj )
        if isinstance( fiksa, dict): 
            print( f"Feature geometry {ii} ID {fiksa['feature_id'] } må fikses") 

            # Output fra json.dumps gir masse escape-tegn for doble quotes, eks { \\"type\\" : \\"POINT\\" 
            # Dette ser ikke ut til å samsvare med output fra databasen
            # output.append( f"UPDATE feature_geometry set geometry = {json.dumps( fiksa['geometry'] )} WHERE id = {fiksa['feature_id']} ;" )

            # Er denne serialieringen OK? Testes
            output.append( f"UPDATE feature_geometry set geometry = '{ fiksa['geometry'] }' WHERE feature_id = '{fiksa['feature_id']}' ;" )

    return output 

if __name__ == '__main__': 
    pass 

