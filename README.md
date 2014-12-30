readme.md
===========
Grass In (short for input to grasshopper)

This project is an input to the grasshopper project.  This code creates address point data based on the US Census Address Feature Class shape files ([found here](http://ftp2.census.gov/geo/tiger/TIGER2014/ADDRFEAT/)). The point locations are used as sample data input into grasshopper.  the code works on stock census data.

Dependencies
------------
Data
- Census Placeec shapefiles (for interested states)
- Census Addr_feat shapefiles (for interested counties)

Software/Libraries
- PostGIS (with access to an equidistant projection - this uses - epsg 3786)
- Python
- Psycopg2

Workflow
--------
1). use [the prep_census_addrfeat.py](https://github.com/feomike/grass_in/blob/master/prep_census_addrfeat.py) code to download and prep two kinds of feature classes required for this project;
	1.1 - the [placeec](http://ftp2.census.gov/geo/tiger/TIGER2014/PLACEEC/) features provide the city names for a complete address
	1.2 - the [address features](http://ftp2.census.gov/geo/tiger/TIGER2014/ADDRFEAT/) provide the linework with address ranges
	
	The [the prep_census_addrfeat.py](https://github.com/feomike/grass_in/blob/master/prep_census_addrfeat.py) code downloads (ftp get) an entire state of data.  All the addr_feat layers are based on county tiles, so this code cycles through a state and grabs each available county.  It then unzip's the download file, imports the resulting shapefile into postgis, adds the city and state field (and county as well) on to the newly imported addr_feat table.  Finally it populates the city, county and state fields.  The city field is populated from an intersect of the centroid of the roads with each polygon in the placeec table to acquire the city name for each place.  These fields are required to make a full example address (e.g. 123 Main St Newport RI 02840)
	
2) use the [build_addr.py](https://github.com/feomike/grass_in/blob/master/build_address.py) code to generate points for every address range on each side of the street for each feature with from/to ranges and fullnames (eg street names).  the code cycles through every line, it works on the left side first, then the right side.  it interpolates points at equal percent ranges along the street.  then, using the direction of the line, offsets the newly generated point at a perpendicular angle to the interpolated point and offset's it by 10 meters.  It projects the points from 4269 to an equidistant projection, then back again to attempt to get a consistent 10 meter offset on each side.  finally it updates the complete address (concatenation of the newly interpolated street number + full address + city and zip) and the original tlid to the newly offset point.

Issues
------
- the code skips any line / side combination w/o both a from and a to address range.  so if it has a from but no to range, then no new address points are generated.  this results in potentially missing real addresses
- the code skips any rows with non-numeric values in the from/to fields. this too potentially misses real addresses.
- the code generates the offset based on trigonometry.  the trig uses the azimuth direction of the full line to create perpendicular movement of an interpolated point along the line.  if the line is s, u shaped or curved, the offsets will not be generated as well as strait lines. and enhancement would be if the azimuth direction was generated off of the nearest vertices to the interpolated point.
- this code generates points for all evan (or odd) values on either side of the street.  so it assumes that a side only has evan or odd numbered addresses.  
- this code generates all possible points within the range of addresses in the from and to fields
- this code accepts exactly what is in the census stock data, and uses no other inputs.  an enhancement would be the using the USPS (or similar) service to determine that a given address is real, and then only build points for them.
- this code uses only percent of available addresses along a line to determine the interpolation percent along a line.  as such the spatial accuracy of the address is suspect, but the likelihood that the point is in on the correct side of the street is high.
