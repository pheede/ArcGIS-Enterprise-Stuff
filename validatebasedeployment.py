"""This script validates an ArcGIS Enterprise deployment to ensure it is
configured properly with all the required components such as Portal for ArcGIS,
ArcGIS Server, ArcGIS Data Store and the associated configuration."""

# Author: Philip Heede <pheede@esri.com>
# Last modified: 2017-02-17

import os
import sys
import ssl
import socket
import urllib.request
import getopt
import getpass
import json
import traceback

def main(argv):
    currentHost = socket.getfqdn().lower()
    currentDir = os.getcwd()
    portalHost = ''
    context = ''
    adminUsername = ''
    adminPassword = ''
    outputDir = ''
    token = ''
    if len(sys.argv) > 0:
        try:
            opts, args = getopt.getopt(argv, "?hn:c:u:p:t:", ("help", "portalurl=", "context=", "user=", "password=", "token=", "ignoressl"))
        except:
            print('One or more invalid arguments')
            print('validatebasedeployment.py [-n <portal hostname>] [-c <portal context>] [-u <admin username>] [-p <admin password>] [-t <token>]')
            sys.exit(2)

        for opt, arg in opts:
            if opt in ('-n', '--portalurl'):
                portalHost = arg
            elif opt in ('-c', '--context'):
                context = arg
            elif opt in ('-u', '--user'):
                adminUsername = arg
            elif opt in ('-p', '--password'):
                adminPassword = arg
            elif opt == '--ignoressl':
                # disable SSL certificate checking to avoid errors with self-signed certs
                # this is NOT a generally recommended practice
                _create_unverified_https_context = ssl._create_unverified_context
                ssl._create_default_https_context = _create_unverified_https_context
            elif opt in ('-t', '--token'):
                token = arg
            elif opt in ('-h', '-?', '--help'):
                print('validatebasedeployment.py [-n <portal hostname>] [-c <portal context>] [-u <admin username>] [-p <admin password>] [-t <token>]')
                sys.exit(0)

    # Prompt for portal hostname
    if portalHost == '':
        portalHost = input('Enter ArcGIS Enterprise FQDN [' + currentHost + ']: ')
        if portalHost == '': portalHost = currentHost

    # Prompt for portal context
    if context == '':
        context = input('Enter context of the portal instance [\'arcgis\']: ')
        if context == '': context = 'arcgis'

    # Prompt for admin username
    if adminUsername == '' and token == '':
        while adminUsername == '':
            adminUsername = input('Enter administrator username: ')

    # Prompt for admin password
    if adminPassword == '' and token == '':
        while adminPassword == '':
            adminPassword = getpass.getpass(prompt='Enter administrator password: ')

    portalUrl = 'https://' + portalHost + '/' + context
    if token == '':
        token = generateToken(adminUsername, adminPassword, portalHost, portalUrl)
        if token == 'Failed':
            print('Invalid administrator username or password.')
            sys.exit(1)

    portalSelf = getPortalSelf(portalUrl, token)
    supportsHostedServices = portalSelf['supportsHostedServices']
    supportsSceneServices = portalSelf['supportsSceneServices']

    federatedServers = getFederatedServers(portalUrl, token)
    hostingServer = None
    for server in federatedServers:
        if 'serverRole' in server:
            serverRole = server['serverRole']
            if serverRole == 'HOSTING_SERVER': hostingServer = server
    
    print()
    print("ArcGIS Enterprise deployment characteristics")
    print("- Hosting server configured: %s" % (hostingServer is not None))
    if hostingServer is None: print("-- WARNING: lack of a hosting server will prevent many functions from working")
    else:
        hasRelationalDataStore = checkArcGISDataStoreRelational(hostingServer['adminUrl'], hostingServer['url'], token)
        print("- ArcGIS Data Store (relational) configured with hosting server: %s" % hasRelationalDataStore)
        if not hasRelationalDataStore: print("-- WARNING: you must use ArcGIS Data Store to configure a relational database")

        analysisServiceStarted = checkAnalysisServices(hostingServer['url'], token)
        print("- Hosting server's spatial analysis service is started and available: %s" % analysisServiceStarted)
        if not analysisServiceStarted: print("-- WARNING: analysis service not started or unreachable")

        print("- Hosted feature services are supported: %s" % supportsHostedServices)
        if not supportsHostedServices: print("-- WARNING: this indicates a lack of ArcGIS Data Store configured with the relational data store type")
        print("- Scene services are supported: %s" % supportsSceneServices)
        if not supportsSceneServices: print("-- WARNING: this indicates a lack of ArcGIS Data Store configured with the tile cache data store type")

def checkArcGISDataStoreRelational(serverAdminUrl, serverUrl, portalToken):
    params = {'token':portalToken, 'f':'pjson', 'types':'egdb'}
    request = urllib.request.Request(serverAdminUrl + '/admin/data/findItems', urllib.parse.urlencode(params).encode('ascii'))
    try: response = urllib.request.urlopen(request)
    except:
        request = urllib.request.Request(serverUrl + '/admin/data/findItems', urllib.parse.urlencode(params).encode('ascii'))
        try: 
            response = urllib.request.urlopen(request)
            print('-- WARNING: hosting server administrative endpoint not')
            print('            accessible from this machine; this may cause')
            print('            publishing issues from ArcGIS Pro')
        except:
            print('-- ERROR: unable to reach hosting server administrative endpoint')
            print('          maybe the administrative endpoint is only accessible internally?')
    egdbs = json.loads(response.read().decode('utf-8'))
    if 'error' in egdbs: return False
    else:
        managedegdb = None
        for egdb in egdbs['items']:
            if egdb['info']['isManaged']: managedegdb = egdb
        if managedegdb is None: return False
        return managedegdb['provider'] == 'ArcGIS Data Store'

def checkAnalysisServices(serverUrl, portalToken):
    params = {'token':portalToken, 'f':'json'}
    request = urllib.request.Request(serverUrl + '/rest/services/System/SpatialAnalysisTools/GPServer?%s' % urllib.parse.urlencode(params))
    try:
        response = urllib.request.urlopen(request)
        serviceInfo = json.loads(response.read().decode('utf-8'))
        if 'error' in serviceInfo: return False
        else: return True
    except:
        return False
    
def getFederatedServers(portalUrl, token):
    params = {'token':token, 'f':'json'}
    request = urllib.request.Request(portalUrl + '/portaladmin/federation/servers?%s' % urllib.parse.urlencode(params))
    response = urllib.request.urlopen(request)
    federatedServers = json.loads(response.read().decode('utf-8'))
    if 'servers' not in federatedServers:
        print('Unable to enumerate federated servers. Not an administrator login?')
        sys.exit(1)

    return federatedServers['servers']

def getPortalSelf(portalUrl, token):
    params = {'token':token, 'f':'json'}
    request = urllib.request.Request(portalUrl + '/sharing/portals/self',
                                     urllib.parse.urlencode(params).encode('ascii'))
    response = urllib.request.urlopen(request)
    portalSelf = json.loads(response.read().decode('utf-8'))
    return portalSelf

def generateToken(username, password, portalHost, portalUrl):
    params = {'username':username,
              'password':password,
              'referer':portalUrl,
              'f':'json'}
    try:
        request = urllib.request.Request(portalUrl + '/sharing/rest/generateToken',
                                         urllib.parse.urlencode(params).encode('ascii'))
        response = urllib.request.urlopen(request)
        genToken = json.loads(response.read().decode('utf-8'))
        if 'token' in genToken.keys():
            return genToken.get('token')
        else:
            return 'Failed'
    except urllib.error.URLError as urlError:
        print('Unable to access ArcGIS Enterprise deployment at ' + portalUrl)
        if isinstance(urlError.reason, ssl.SSLError):
            print("SSL certificate validation error. Maybe you're using a self-signed certificate?")
            print("Pass the --ignoressl parameter to disable certificate validation")
        else:
            print(urlError.reason)
        sys.exit(1)
    except Exception as ex:
        print('Unable to access ArcGIS Enterprise deployment at ' + portalUrl)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("*** print_exception:")
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stdout)
        sys.exit(0)

if not sys.version_info >= (3, 4):
    print('This script requires Python 3.4 or higher: found Python %s.%s' % sys.version_info[:2])

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
