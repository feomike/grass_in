#build_address.py
#michael byrne
#consumer finance protection bureau
#dec 24, 2014
#
#this script generates all possible address points off of a us census addr_feat shapefile
#
#dependencies
#arguments:
#	- one county at a time
#data:
#	- import county shapes for addrfeat features into either a single table, or many tables
#	- output table
#software:
#	- psycopg
#	- postgis
#	- math
#	
#variables:
#	- need to figure out projection
#	- offset distance is 10 meters (to start)
#	- database, schema, in-table, out-table


#business flow
#loop through every line feature (e.g. a gid) in a county addrfeat feature table 
#downloaded from us census
#for each line feature:
#	- for each side (left of right)
#		- get all the attributes for that side of the street
#			- from, to, fulladdress, zip, zipplus, and tlid
#		- get the low range (fram number)
#		- get the high range (to number)
#		- calculate the percentage along the line, which is equal to half the number
#			of possible addresses (to - from) / 2
#		- for each possible address in the set on one side
#			- find the xy of the point at this linear interpolation distance percentage
#			- find the direction of the line at that location
#			- find the perpendicular and offset for that percentage
#			- insert a new record with values of an offset point, full address, and tlid 

# Import system modules
import psycopg2  #used for writing sql/postgis
import time #used for seeing how long the code takes
import math #used for trigonometry
import re #used to search a string to see if it contains any non-numbers (e.g. address)
now = time.localtime(time.time())
print "start time:", time.asctime(now)

#variables
myHost = "localhost"
myPort = "54321"
myUser = "postgres"
db = "feomike"
schema = "census"
theTBL = "powel" #"tl_2014_05119_addrfeat" #"tl_2014_44005_addrfeat" #"powel" #"working" 
idFld = "tlid"
in_Prj = "4269"
#http://spatialreference.org/ref/epsg/3786/
eqd_prj = "3786"

#make the output table; this is the table which will contain all the output points
#arguments: 
#-myTab os the table you are operating on; a new table will be created w/ this suffix
#-myCur is the cursor to use to execute the query
def mk_Tbl(myTab, myCur):
	mySQL = "DROP TABLE IF EXISTS " + schema + ".mbadd_" + myTab + "; COMMIT;"
	myCur.execute(mySQL)
	mySQL = "CREATE TABLE " + schema + ".mbadd_" + myTab 
	mySQL = mySQL + "( gid serial not null, "
	mySQL = mySQL + idFld + " numeric (10, 0), " 
	mySQL = mySQL + "address character varying, "
	mySQL = mySQL + "geom geometry(Point, " + in_Prj + "), "
	mySQL = mySQL + "CONSTRAINT " + schema + "_mbadd_" + myTab + "_gid PRIMARY KEY (gid), "
	mySQL = mySQL + "CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2), "
	mySQL = mySQL + "CONSTRAINT enforce_geotype_geom CHECK (geometrytype(geom) = 'POINT'::text), "
	mySQL = mySQL + "CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = " + in_Prj + ") "
	mySQL = mySQL + ") WITH (OIDS=TRUE); ALTER TABLE " + schema + ".mbadd_" + myTab
	mySQL = mySQL + " OWNER TO postgres; COMMIT; "
	myCur.execute(mySQL)
	return()

#is_num is a function that returns the boolean of resulting test to see if the 
#inputed string contains only numbers
#arguments:
#- myStr
def isNum(myStr):
	myBol = True
#	theList = ["A","B","C","D","E","F","G","H","I","J","K","L","M"]
#	theList = theList + ["N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
#	theList = theList + ["-"]
#	for c in theList:
#		if c in myStr:
#			myBol = False
	match = re.search("^\d+$", myStr)
	try: myStr = match.group(0)
	except: myBol = False
	return myBol
#return the direction of that line for proper offsetting
#arguments:
#- myID is the unique ID of the row you are operating on
def get_az(myID):
	#first get the starting and ending point of the line
	mySQL = "SELECT ST_X(ST_StartPoint((ST_Dump(geom))" 
	mySQL = mySQL + ".geom)), "
	mySQL = mySQL + "ST_Y(ST_StartPoint((ST_Dump(geom))"
	mySQL = mySQL + ".geom)), " 	
	mySQL = mySQL + "ST_X(ST_EndPoint((ST_Dump(geom))" 
	mySQL = mySQL + ".geom)), "
	mySQL = mySQL + "ST_Y(ST_EndPoint((ST_Dump(geom))"
	mySQL = mySQL + ".geom)) " 
	mySQL = mySQL + "FROM " + schema
	mySQL = mySQL + "." + theTBL + " WHERE gid = "
	mySQL = mySQL + str(myID) + "; " 
	#execute the query, get the returned point 
	someCur.execute(mySQL)
	if someCur.rowcount == 1:
		myrow = someCur.fetchone()
		theSX = myrow[0]
		theSY = myrow[1]
		theEX = myrow[2]
		theEY = myrow[3]
	else:
		theSX = 0
		theSY = 0
		theEX = 90
		theEY = 90
	#next get the azimuth of the line from these two points
	mySQL = "SELECT degrees(ST_Azimuth(ST_GeomFromText(' "
	mySQL = mySQL + "POINT( " + str(theSX) + " " + str(theSY) + ")'," + in_Prj + "),"
	mySQL = mySQL + "ST_GeomFromText('POINT("
	mySQL = mySQL + str(theEX) + " " + str(theEY) + ")'," + in_Prj + ") )); "
	#execute the query, get the returned direction 
	someCur.execute(mySQL)
	if someCur.rowcount == 1:
		myrow = someCur.fetchone()
		myDir = myrow[0]
		#make sure the myDir is set to something
		if myDir == None:
			myDir = 180
	return(myDir)

#return the direction of that line for proper offsetting
#arguments:
#- myID is the unique ID of the row you are operating on
def get_az_xy(theSX,theSY,theEX,theEY ):
	#next get the azimuth of the line from these two points
	mySQL = "SELECT degrees(ST_Azimuth(ST_GeomFromText(' "
	mySQL = mySQL + "POINT( " + str(theSX) + " " + str(theSY) + ")'," + in_Prj + "),"
	mySQL = mySQL + "ST_GeomFromText('POINT("
	mySQL = mySQL + str(theEX) + " " + str(theEY) + ")'," + in_Prj + ") )); "
	#execute the query, get the returned direction 
	someCur.execute(mySQL)
	if someCur.rowcount == 1:
		myrow = someCur.fetchone()
		myDir = myrow[0]
		#make sure the myDir is set to something
		if myDir == None:
			myDir = 180	
	return(myDir)

#make an address from the values of street, zip and zip+4
#arguments:
#- myst the fullname of the street name
#- myzip is the zip code
#- myz4 is the zip +4 if there is one
#- mycity is the city
#- myST is the state abbreviation
def ret_add(myst, myzip, myz4, mycity, myST):
	#is myst has a single quote in, you need to escape it so later you can insert it 
	#into a table, and sql needs the single quote escaped
	if "'" in myst:
		myst = myst.replace("'", "''")
	if myzip == None:
		myzip = " " 
	if mycity == None:
		mycity = " "
	if myz4 is None:
		myadd = myst + " " + mycity + " " + myST + " " + myzip
	else:
		myadd = myst + " " + mycity + " " + myST + " " + myzip + "-" + myz4
	return myadd

#make the set of points on the line
#document each of these arguments
#arguments are myCur - cursor to use 
#num is number numer of addresses to make
#myID is the unique id of the line feature
#house is the base house number to start with
#myAdd is the street + zip
#side is the side of the street (left of right)
#arguments:
#-num is the number of address to be generated (eg to - from)
#-myID is the unique ID of the row you are operating on
#-house is the starting house number
#-myadd is the full address minus the house number (e.g. Main St Somecity, CA 95616
#-side is left of right; the side of the street to grow on
#-myDir is the azimuth direction of the street in degrees 0 - 360
def mk_pts(num, myID, house, myadd, side, myAz, myGID, myDir, myFirst):
	#this section intends to come up with the percentage along the line to generate 
	#a point.  the percent is +1 of the number of addresses to do along that line
	#(e.g. if n=3, points should be generated every 25%)
	pct = (100 / (num + 1 )) / 100 #(originally num - 1
	#loop through for every address point to be generated
	myCnt = 1 #this number is multiplied by pct to get an 
	          #absolute percentage along the line 
	myHouse = int(house)
	#set up a cursor for this one
	myCur = conn.cursor()
	theend = False
	while myCnt < (num + 1):
		#format a sql statement to obtain the point as a percentage along the line
		#return the values for the point, so you can insert them as 
		#geometry in a row later
		#i am projecting the stock census 4269 epsg to an equal distant projection
		#to get the offset reasonably correct (e.g. dealing in meters rather than dd)
		mySQL = "SELECT ST_X(ST_Transform(ST_Line_Interpolate_Point((ST_Dump(geom))" 
		mySQL = mySQL + ".geom, " + str(pct*myCnt) + "), " + eqd_prj + ")), "
		mySQL = mySQL + "ST_Y(ST_Transform(ST_Line_Interpolate_Point((ST_Dump(geom))"
		mySQL = mySQL + ".geom, " + str(pct*myCnt) + "), " + eqd_prj + ")) " 	
		mySQL = mySQL + "FROM " + schema
		mySQL = mySQL + "." + theTBL + " WHERE gid = "
		mySQL = mySQL + str(myGID) + "; " 
		#execute the query, get the returned point 
		myCur.execute(mySQL)
		#there should be only one row returned, as we are using the unique id field
		if myCur.rowcount == 1:
			myrow = myCur.fetchone()
			theX = myrow[0]
			theY = myrow[1]
		#if this happens, there is an error, but it should still work
		else:
			theX = 0
			theY = 0
		#this next line is here and commented out to test for perpendicularity of 
		#the point along the line, against the offset point
#		#insert_pt(myID, str(myHouse) + " " + myadd, theX, theY)
#		print "     theX value is: " + str(theX) + " theY value is: " + str(theY)
		#3 - move the point using some trig and convert it back to input prj
		#4 - insert the point into a new row, complete w/ full attributes
		thePt = offset_pt(theX, theY, side, myAz)
		newX = thePt[0]
		newY = thePt[1]
		if myFirst:
			if myCnt > 1:
				f.write(",\n")
		else:
			f.write(",\n")
		myFirst = False
		#insert_pt(myID, str(myHouse) + " " + myadd, newX, newY)
		write_geoJson(myGID, myID, str(myHouse) + " " + myadd, newX, newY)
		#myHouse always gets incremented by two
		if myDir == "forward":
			myHouse = myHouse + 2
		else:
			myHouse = myHouse - 2
		myCnt = myCnt + 1
	myCur.close()
	return()

#offset the points to the appropriate side based on the direction of the arc and the 
#side that these addresses should be on.  this function uses trigonometry to move a 
#a delta x and and delta y.  
#then return new X and Y as a list
#arguments:
#-myX is the X value of the input point
#-myY is the Y value of the input point
#-mySide is the side of the line (left or right)
#-myAz is the azimuth direction of the line (0-360)
def offset_pt(myX, myY, mySide, myAz):
	#we are offsetting by 5 meters
	theOffset = 10
	#a perpendicular line on the left side is always -90 degrees from the original
	#direction of the line
	if mySide == "left":
		newDir = myAz - 90
	#a perpendicular line on the right side is always +90 degrees from the original
	#direction of the line
	if mySide == "right":
		newDir = myAz + 90
	#do do some basic trig to get the delta X and the delta y of the new point.
	#assuming the new perpendicular line is the hypotenuse, then the new point is 
	#delta X is the distance along the X access or the adjacent line in a right triangle
	#here, we have to use the math.radians to convert the new direction to and then 
	#pass that to the sin method; similarly, delta Y is the the opposite line in a right
	#triangle, so we use cosine of that angle to acquire this distance.
	newX = myX + (math.sin(math.radians(newDir)))*theOffset
	newY = myY + (math.cos(math.radians(newDir)))*theOffset
	#transform the xy back to 4269, and return, then retest
	#all of newport county took - start time: Thu Jan  1 16:47:09 2015
     #                                   going to be operating on this many segments: 7284
	#                             end time: Thu Jan  1 16:52:05 2015
	mySQL = "SELECT  " 
	mySQL = mySQL + "st_x(st_transform(ST_GeomFromText('POINT(" + str(newX) + " " + str(newY) + ")', " + eqd_prj + "), " + in_Prj + " )), "
	mySQL = mySQL + "st_y(st_transform(ST_GeomFromText('POINT(" + str(newX) + " " + str(newY) + ")', " + eqd_prj + "), " + in_Prj + " )); "	
	someCur.execute(mySQL)
	myrow = someCur.fetchone()
	newX = myrow[0]
	newY = myrow[1]	
	return ([newX, newY])
	
#insert the new point as a row
def insert_pt(myID, myadd, myX, myY):
	mySQL = "INSERT INTO " + schema + ".mbadd_" + theTBL + " (" + idFld + ", address, geom) "
	mySQL = mySQL + "VALUES ("
	mySQL = mySQL + str(myID) + ", "
	mySQL = mySQL + "'" + myadd + "', "
	mySQL = mySQL + "st_transform(ST_GeomFromText('POINT(" + str(myX) + " " + str(myY) + ")', " + eqd_prj + "), 4269) "
	mySQL = mySQL + "); COMMIT;"
	updCur.execute(mySQL)

#instead of writing a db, lets write to a geoJson file
def write_geoJson(myGID, myTLID, myAdd, myX, myY):
	#example is { "type": "Feature", "id": 1, "properties": 
	#example    { "gid": 1, "tlid": 47349507.000000, "address": "48 Powel Ave Newport RI 02840" }, 
	#example            "geometry": { "type": "Point", "coordinates": [ -71.304186, 41.495393 ] } }
     myjson = "{ \"type\": \"Feature\", \"id\": " + str(myGID) + ", \"properties\": { "
     myjson = myjson + "\"tlid\": " + str(myTLID) + ", \"address\": \"" + myAdd + "\" },"
     myjson = myjson + "\"geometry\":{\"type\": \"Point\", \"coordinates\": [ "
     myjson = myjson + str(myX) + ", " + str(myY) + "] } }"
     f.write(myjson + "\n")

try:
	#set up the connection to the database
	myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
	conn = psycopg2.connect(myConn)
	theCur = conn.cursor() #used for loop queries
	updCur = conn.cursor() #used for updating rows
	someCur = conn.cursor() #used for single select statements
	#make new table to insert, based on the input table name
	mk_Tbl(theTBL, someCur)
	#get the total number of records to go through
	#azimuth is a tricky one b/c the input is a multi-line string, not a line string
	theSQL = "SELECT tlid, lfromhn, ltohn, rfromhn, rtohn, fullname, zipl, zipr, "
	theSQL = theSQL + "plus4l, plus4r, city, st_abbr, gid, "
	theSQL = theSQL + "degrees(ST_Azimuth("
	theSQL = theSQL + "ST_StartPoint(ST_GeometryN(ST_Multi(geom),1)),"
	theSQL = theSQL + "ST_EndPoint(ST_GeometryN(ST_Multi(geom),1))"
	theSQL = theSQL + ")) as azimuth"
	#here insert the start end point of the line, to only make the query once
	#you need this for getAZ
	theSQL = theSQL + " from " + schema + "." + theTBL 
	theSQL = theSQL + ";"
	theCur.execute(theSQL)
	thenum = theCur.rowcount
	print "     going to be operating on this many segments: " + str(thenum)
	counter = 1
	first = True 
	myFile = "mbadd_" + theTBL + ".geojson"
	f = open(myFile,'w')
	myStr = "{\"type\": \"FeatureCollection\", \"features\":[ "
	f.write (myStr + "\n")
	for row in theCur:
		#get all the necessary field data out of the record for the cursor
		#values are as follows
		#0:tlid, 1:lfrom, 2:lto, 3:rfrom, 4:rto, 5:st, 6:lzip, 
		#7:rzip, 8:lzip4, 9:lzip4, 10:city, 11:st_abbr, 12:gid
		#13:azimuth
#		print "working on tlid: " + str(row[0])
		doL = 0
		lfrom = row[1]
		lto = row[2]
		rfrom = row[3]
		rto = row[4]
		azimuth = row[13]
		#for each row, get the direction of the line; pass in the gid
		#azimuth can be faster if we add the get select onto the loop query + 
		#put it under a conditional for the l/r from/to being non-null
#		if (lfrom is not None and lto is not None) or (rfrom is not None and rto is not None):
#			azimuth = get_az_xy(row[13], row[14], row[15], row[16])
#			print "function azimuth is: " + str(azimuth)
		#make sure there is a value in the left address field
		if lto is not None and lfrom is not None:
			add = ret_add(row[5], row[6], row[8], row[10], row[11])
			#make sure the left address values are only numbers
			if isNum(lfrom) and isNum(lto):
				lCnt = round(float(int(lto)- int(lfrom) + 1) / float(2))
				#if from > to, then need to go in reverse
				if lCnt > 0:
					ldir = "forward"
				else:
					ldir = "reverse"
					lCnt = abs(lCnt)
				doL = 1
			#for the left side, create the desired number of address points
			#only do this if there is an address range (values in both from and to fields)
			if doL == 1:
				mk_pts(lCnt, row[0], row[1], add, "left", azimuth, row[12], ldir,first)
				first = False

		doR = 0
		#make sure there is a value in the left address field
		if rto is not None and rfrom is not None:
			#make sure the left address values are single numbers
			#make sure the left address values are only numbers
			add = ret_add(row[5], row[7], row[9	], row[10], row[11])
			if isNum(rfrom) and isNum(rto):
				rCnt = round(float(int(rto)- int(rfrom) + 1) / float(2))
				#if from > to, then need to go in reverse
				if rCnt > 0:
					rdir = "forward"
				else:
					rdir = "reverse"
					rCnt = abs(rCnt)
				doR = 1
			#for the left side, create the desired number of address points
			#only do this if there is an address range (values in both from and to fields)
			if doR == 1:
				mk_pts(rCnt, row[0], row[3], add, "right", azimuth, row[12], rdir,first)
				first = False
		counter = counter + 1
		
	f.write("]}" + "\n")
	f.close()
	updCur.close()
	theCur.close()
	del updCur, theCur
	now = time.localtime(time.time())
	print "end time:", time.asctime(now)
except:
	print "bad thing happened"	
