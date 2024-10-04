class bikePoint(object):
    def __init__(self, lat, lng) -> None:
        self.lat = lat
        self.lng = lng

    def bikePoint2dict(point):
        return {
            'lat':point.lat,
            'lng':point.lng,
        }
    def bikePoint2list(point):
        return [point.lat,point.lng]

    def bikePoint2Geometry (point,srid):
        lat = str(point.lat)
        lng = str(point.lng)
        GeoPoint = "ST_GeomFromText('POINT("+lat + ' ' +lng +")',"+str(srid)+')'
        return GeoPoint

    def bikePoint2Line(points,srid):
        GeoLine ="ST_GeomFromText('LINESTRING("
        for i in range(len(points)):
            if i == len(points)-1:
                GeoLine+=str(points[i].lat)+' '+str(points[i].lng)+")',%s)"%(srid)
            else:
                GeoLine+=str(points[i].lat)+' '+str(points[i].lng)+","
        return GeoLine

def getseason(Month):
    if Month>=1 and Month <=3:
        return 1
    elif Month>=4 and Month <=6:
        return 2
    elif Month>=7 and Month <=9:
        return 3
    else:
        return 4
