"""
Dekoder databasedump fra mariadbpython.py
"""

import pickle
import json 


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
    
    assert dfgeom['srid'] == 25833, f"Ikke implementert støtte for SRID={dfgeom['srid']} ennå"
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