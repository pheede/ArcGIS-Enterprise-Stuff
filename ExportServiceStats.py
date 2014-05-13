# Demonstrates how to export service statistics to a CSV file.
# ArcGIS Server 10.3 or higher

# For HTTP calls
import httplib, urllib, urllib2, json
# For time-based functions
import time, uuid
# For system tools
import sys, os
# For reading passwords without echoing
import getpass
# For writing csv files
import csv

# Defines the entry point into the script
def main(argv=None):
    # Print some info
    print("")
    print("This tool demonstrates how to export service statistics to a CSV file.")
    print("")
    
    # Ask for admin/publisher user name and password
    username = raw_input("Enter user name: ")
    password = getpass.getpass("Enter password: ")
    
    # Ask for server name
    serverName = raw_input("Enter server name: ")
    serverPort = 6080 # assumes server is enabled for HTTP access, HTTPS only sites will require (minor) script changes

    # Ask for FromTime
    fromTime = 0
    while fromTime == 0:
        fromTime = raw_input("Start date and time of report in YYYY-MM-DD HH:MM format (e.g. 2014-05-10 14:00): ")
        # Convert input to Python struct_time and then to Unix timestamp in ms
        try: fromTime = int(time.mktime(time.strptime(fromTime, '%Y-%m-%d %H:%M'))*1000) 
        except:
            print('Unable to parse input. Ensure date and time is in YYYY-MM-DD HH:MM format')
            fromTime = 0
    
    # Ask for ToTime
    toTime = 0
    while toTime == 0:
        toTime = raw_input("End date and time of report in YYYY-MM-DD HH:MM format (e.g. 2014-05-10 14:00): ")
        # Convert input to Python struct_time and then to Unix timestamp in ms
        try: toTime = int(time.mktime(time.strptime(toTime, '%Y-%m-%d %H:%M'))*1000)
        except: 
            print('Unable to parse input. Ensure date and time is in YYYY-MM-DD HH:MM format')
            toTime = 0
    
    # Ask for time interval
    interval = int(raw_input("Time interval to report statistics (in minutes): "))
    
    # Ask for service name
    serviceName = raw_input("Service name and type.  If the service is nested in a folder, include the folder name (for example, planning/firehydrants.MapServer): ")
    if not serviceName.startswith('services/'): serviceName = "services/" + serviceName
    
    # Ask for output csv file name
    fileName = raw_input("Enter the name of the output CSV file to be created: ")
    if os.path.splitext(fileName)[1] == '': fileName = fileName + ".csv"
    
    # Get a token
    token = getToken(username, password, serverName, serverPort)
    if token == "":
        print("Could not generate a token with the username and password provided.")
        return

    # Construct URL to query the logs
    statsCreateReportURL = "http://{0}:{1}/arcgis/admin/usagereports/add".format(serverName, serverPort)

    # Create unique name for temp report
    reportName = uuid.uuid4().hex 

    # Create report JSON definition
    statsDefinition = {'reportname' : reportName, 'since' : 'CUSTOM', 
           'queries' : [{'resourceURIs' : [serviceName],
           'metrics' : ['RequestCount', 'RequestsFailed', 'RequestsTimedOut', 
           'RequestMaxResponseTime', 'RequestAvgResponseTime'] }],
           'from' : fromTime, 'to': toTime, 'aggregationInterval' : interval,
           'metadata' : {'temp' : True, 'tempTimer' : int(time.time() * 1000)}}

    postdata = { 'usagereport' : json.dumps(statsDefinition) }
    createReportResult = postAndLoadJSON(statsCreateReportURL, token, postdata)
    
    # Query newly created report
    statsQueryReportURL = "http://{0}:{1}/arcgis/admin/usagereports/{2}/data".format(serverName, serverPort, reportName)
    postdata = { 'filter' : { 'machines' : '*'} }
    reportData = postAndLoadJSON(statsQueryReportURL, token, postdata)
    print(reportData)
    
    # Get list of timeslices covered by this report
    timeslices = reportData['report']['time-slices']
    print(timeslices)
    
    header = [serviceName]
    for timeslice in timeslices:
        t = time.localtime(timeslice/1000.0)
        header.append(time.strftime('%Y-%m-%d %H:%M', t))
    
    # Open output file
    output = open(fileName, 'wb')
    csvwriter = csv.writer(output, dialect='excel')
    csvwriter.writerow(header)
    
    # Dig into the report for the data for individual services
    print(reportData)
    for serviceMetric in reportData['report']['report-data'][0]:
        name = serviceMetric['metric-type']
        data = serviceMetric['data']
        csvwriter.writerow([name] + data)
    
    output.close()
    
    # Cleanup (delete) statistics report
    statsDeleteReportURL = "http://{0}:{1}/arcgis/admin/usagereports/{2}/delete".format(serverName, serverPort, reportName)
    deleteReportResult = postAndLoadJSON(statsDeleteReportURL, token)
    
    print("Export done!")

    return

# A function that makes an HTTP POST request and returns the result JSON object
def postAndLoadJSON(url, token = None, postdata = None):
    if not postdata: postdata = {}
    # Add token to POST data if not already present and supplied
    if token and 'token' not in postdata: postdata['token'] = token 
    # Add JSON format specifier to POST data if not already present
    if 'f' not in postdata: postdata['f'] = 'json' 
    
    # Encode data and POST to server
    postdata = urllib.urlencode(postdata)
    response = urllib2.urlopen(url, data = postdata)
    
    if (response.getcode() != 200):
        response.close()
        raise Exception('Error performing request to {0}'.format(url))

    data = response.read()
    response.close()

    # Check that data returned is not an error object
    if not assertJsonSuccess(data):          
        raise Exception("Error returned by operation. " + data)

    # Deserialize response into Python object
    return json.loads(data)

#A function to generate a token given username, password and the adminURL.
def getToken(username, password, serverName, serverPort):
    # Token URL is typically http://server[:port]/arcgis/admin/generateToken
    tokenURL = "/arcgis/admin/generateToken"
    
    # URL-encode the token parameters
    params = urllib.urlencode({'username': username, 'password': password, 'client': 'requestip', 'f': 'json'})
    
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    
    # Connect to URL and post parameters
    httpConn = httplib.HTTPConnection(serverName, serverPort)
    httpConn.request("POST", tokenURL, params, headers)
    
    # Read response
    response = httpConn.getresponse()
    if (response.status != 200):
        httpConn.close()
        print("Error while fetching tokens from admin URL. Please check the URL and try again.")
        return
    else:
        data = response.read()
        httpConn.close()
        
        # Check that data returned is not an error object
        if not assertJsonSuccess(data):            
            return
        
        # Extract the toke from it
        token = json.loads(data)       
        return token['token']            

#A function that checks that the input JSON object
#  is not an error object.    
def assertJsonSuccess(data):
    obj = json.loads(data)
    if 'status' in obj and obj['status'] == "error":
        print("Error: JSON object returns an error. " + str(obj))
        return False
    else:
        return True
    
        
# Script start
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
