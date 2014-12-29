#prep_census_addrfeat.py
#process roads + placec
#michael byrne
#december 26, 2014
#this script preps the census addr_feat and placec shapes for prepocessing
#into a postgress container.  the pre-processing is primarily to add two fields
#(city and state) on to the addr_feat feature class and populated
#these two fields are the last two required fields to make a full address

#here is the process
#argument for each state
#ftp acquire the placec zip file
#unzip the placec zip file
#using shp2pgsql, import the shapefile into postgress
#add a field called state onto the placec and populate it w/ the shape two letter abbr
#for each counter less than 500
#	check to see if the county addr_feat exits, if so, download that county
#	import the county file into pg w/ shp2pgsql
#	then add these fields
#		county fips, state fips, statename, city
#after all counties are imported,
#	cycle through all place names and update the city name on roads database

import os
import psycopg2
import time
now = time.localtime(time.time())
print "start time:", time.asctime(now)

#variables
myHost = "localhost"
myPort = "54321"
myUser = "postgres"
db = "feomike"
schema = "census"

yr = "2014"
pl_feat = "placeec" #PLACEEC
addr_feat = "addrfeat" #ADDRFEAT
dir = "ftp://ftp2.census.gov/geo/tiger/TIGER" 
prj = "4269"

#clean up pg to make sure there isn't an over write code
def clean_pg(myST, myFeat):
	if myFeat == "placeec":
		myFile = 	"tl_2012_" + st + "_placeec"
	mySQL = "DROP TABLE IF EXISTS " + schema + "." + myFile + "; COMMIT; "
	theCur.execute(mySQL)
	return()

#download the zip file from the appropriate location, unzip and import into gp
def get_st_feat(myST, myFeat):
	if myFeat == "placeec":
		myfile = "tl_2012_" + st + "_placeec" 
		os.system("ftp " + dir + yr + "/PLACEEC/" + myfile + ".zip")
		if os.path.isfile(myfile + ".zip"):
			os.system("unzip " + myfile + ".zip")
			shp_args = "-s " + prj +  " -g geom " + myfile + ".shp " + schema + "." 
			shp_args = shp_args + myfile + " | psql -p " 
			shp_args = shp_args + myPort + " -h " + myHost + " " + db
			os.system("shp2pgsql " + shp_args)
			#create index's on table
			mySQL = "CREATE INDEX " + schema + "_" + myfile + "_geom_gist ON "
			mySQL = mySQL + schema + "." + myfile + " USING gist (geom); "
			theCur.execute(mySQL)		
			fls = {"zip", "dbf", "prj", "shp", "xml", "shx"}
			for fl in fls:
				os.system("rm *." + fl)
	return()


#download the zip file from the appropriate location and unzip
def get_cty_feat(myST, myCty, myFeat):
	if len(str(cty)) == 1:
		num = "00" + str(cty)
	if len(str(cty)) == 2:
		num = "0" + str(cty)
	if len(str(cty)) == 3:
		num = str(cty)
	if myFeat == "addrfeat":
		myfile = "tl_2014_" + st + num + "_addrfeat" 
		os.system("ftp " + dir + yr + "/ADDRFEAT/" + myfile + ".zip")
		if os.path.isfile(myfile + ".zip"):
			mySQL = "DROP TABLE IF EXISTS " + schema + "." + myfile + "; COMMIT; "
			theCur.execute(mySQL)
			os.system("unzip " + myfile + ".zip")
			shp_args = "-s " + prj +  " -g geom " + myfile + ".shp " + schema + "." 
			shp_args = shp_args + myfile + " | psql -p " 
			shp_args = shp_args + myPort + " -h " + myHost + " " + db
			os.system("shp2pgsql " + shp_args)
			mySQL = "CREATE INDEX " + schema + "_" + myfile + "_geom_gist ON "
			mySQL = mySQL + schema + "." + myfile + " USING gist (geom); "
			mySQL = mySQL + "CREATE INDEX " + schema + "_" + myfile + "_tlid_btree ON "
			mySQL = mySQL + schema + "." + myfile + " USING btree (tlid); "
			theCur.execute(mySQL)			
			fls = {"zip", "dbf", "prj", "shp", "xml", "shx"}
			for fl in fls:
				os.system("rm *." + fl)			
			#add field on to county for st_fips, cty_fips, st_abbr, and city
			mySQL = "ALTER TABLE " + schema + "." + myfile + " ADD COLUMN st_abbr "
			mySQL = mySQL + "character varying(2); "
			mySQL = mySQL + "ALTER TABLE " + schema + "." + myfile + " ADD COLUMN st_fips "
			mySQL = mySQL + "character varying(2); "
			mySQL = mySQL + "ALTER TABLE " + schema + "." + myfile + " ADD COLUMN cty "
			mySQL = mySQL + "character varying(3); "
			mySQL = mySQL + "ALTER TABLE " + schema + "." + myfile + " ADD COLUMN cty_fips "
			mySQL = mySQL + "character varying(5); "
			mySQL = mySQL + "ALTER TABLE " + schema + "." + myfile + " ADD COLUMN city "
			mySQL = mySQL + "character varying(100); COMMIT; "
			theCur.execute(mySQL)
			
			#populate the fields
			st_abbr = ret_st_abbr(myST)
			mySQL = "UPDATE " + schema + "." + myfile + " SET st_abbr = '" 
			mySQL = mySQL + st_abbr + "'; "
			mySQL = mySQL + "UPDATE " + schema + "." + myfile + " SET st_fips = '" 
			mySQL = mySQL + myST + "';  "
			mySQL = mySQL + "UPDATE " + schema + "." + myfile + " SET cty = '" 
			mySQL = mySQL + num + "';  "
			mySQL = mySQL + "UPDATE " + schema + "." + myfile + " SET cty_fips = '" 
			mySQL = mySQL + myST + num + "'; COMMIT; "
			theCur.execute(mySQL)
			
			#populate the city name as a function of the intersect 
			#with the placeec feature
			mySQL = "UPDATE " + schema + "." + myfile + " SET city = ( SELECT nameec "
			mySQL = mySQL + " FROM " + schema + ".tl_2012_" + myST + "_placeec " 
			mySQL = mySQL + "WHERE ST_contains(" 
			mySQL = mySQL + "tl_2012_" + myST + "_placeec.geom, "
			mySQL = mySQL + "ST_Centroid(" + myfile + ".geom)) "
			mySQL = mySQL + "); COMMIT; "
			print mySQL
			theCur.execute(mySQL)
			
#return the state abbreviation given the state fips code
def ret_st_abbr(myST):
	st_abbr = "0"
	if myST == "01":
		st_abbr = "AL"
	if myST == "02":
		st_abbr = "AK"
	if myST == "04":
		st_abbr = "AZ"
	if myST == "05":
		st_abbr = "AR"
	if myST == "06":
		st_abbr = "CA"
	if myST == "08":
		st_abbr = "CO"
	if myST == "09":
		st_abbr = "CT"
	if myST == "10":
		st_abbr = "DE"
	if myST == "11":
		st_abbr = "DC"
	if myST == "12":
		st_abbr = "FL"
	if myST == "13":
		st_abbr = "GA"
	if myST == "15":
		st_abbr = "HI"
	if myST == "19":
		st_abbr = "IA"
	if myST == "16":
		st_abbr = "ID"
	if myST == "17":
		st_abbr = "IL"		
	if myST == "18":
		st_abbr = "IN"
	if myST == "20":
		st_abbr = "KS"		
	if myST == "21":
		st_abbr = "KY"
	if myST == "22":
		st_abbr = "LA"
	if myST == "25":
		st_abbr = "MA"
	if myST == "24":
		st_abbr = "MD"
	if myST == "23":
		st_abbr = "ME"
	if myST == "26":
		st_abbr = "MI"
	if myST == "27":
		st_abbr = "MN"
	if myST == "29":
		st_abbr = "MO"
	if myST == "28":
		st_abbr = "MS"
	if myST == "30":
		st_abbr = "MT"
	if myST == "37":
		st_abbr = "NC"
	if myST == "38":
		st_abbr = "ND"
	if myST == "31":
		st_abbr = "NE"
	if myST == "33":
		st_abbr = "NH"
	if myST == "34":
		st_abbr = "NJ"
	if myST == "13":
		st_abbr = "GA"
	if myST == "35":
		st_abbr = "NM"
	if myST == "32":
		st_abbr = "NV"
	if myST == "36":
		st_abbr = "NY"		
	if myST == "39":
		st_abbr = "OH"
	if myST == "40":
		st_abbr = "OK"
	if myST == "41":
		st_abbr = "OR"
	if myST == "42":
		st_abbr = "PA"
	if myST == "44":
		st_abbr = "RI"		
	if myST == "45":
		st_abbr = "SC"
	if myST == "46":
		st_abbr = "SD"
	if myST == "47":
		st_abbr = "TN"
	if myST == "48":
		st_abbr = "TX"
	if myST == "49":
		st_abbr = "UT"
	if myST == "51":
		st_abbr = "VA"
	if myST == "13":
		st_abbr = "GA"
	if myST == "50":
		st_abbr = "VT"
	if myST == "53":
		st_abbr = "WA"
	if myST == "55":
		st_abbr = "WI"
	if myST == "54":
		st_abbr = "WV"
	if myST == "56":
		st_abbr = "WY"
	if st_abbr == "0":
		theMsg = "You likely did not enter a valid two letter state abbreviation,"
		theMsg = theMsg + "please run again"
	return(st_abbr)



#set up the connection to the database
myConn = "dbname=" + db + " host=" + myHost + " port=" + myPort + " user=" + myUser
conn = psycopg2.connect(myConn)
theCur = conn.cursor()

states = {"44"}
for st in states:
	clean_pg(st, pl_feat)
	get_st_feat(st, pl_feat)
	cty = 1
	while cty < 510: #max cty num is 509 in tx - 510
		get_cty_feat(st, cty, addr_feat)
		cty = cty + 1
	theCur.close()
	del theCur
