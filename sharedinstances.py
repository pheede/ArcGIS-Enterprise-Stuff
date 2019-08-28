import sys
import argparse
import urllib

def parseInputParameters():
    parser = argparse.ArgumentParser(description='List and optionally update map services in an ArcGIS Server site.')
    parser.add_argument('--server', help='The URL of the ArcGIS Server site to work against. Only built-in authentication is supported.', required=True)
    parser.add_argument('--user', help='Username of the administrative account', required=True)
    parser.add_argument('--password', help='Password of the specified user account.', required=True)
    parser.add_argument('--update', action='store_true', help='Specify this parameter to change all Pro-based services from dedicated to shared instances.')

    args = parser.parse_args()

    return args

def listServices(server):
    arcmapsvcs = []
    prosvcs = []
    sharedinstancesvcs = []
    for folder in server.services.folders:
        # skip the system folders
        if folder in ['Hosted', 'DataStoreCatalogs', 'System', 'Utilities']: continue

        for service in server.services.list(folder=folder):
            # skip everything that's not a map service; we don't support anything else in the shared pool at 10.7.x
            if service.properties['type'] != 'MapServer': continue

            # the provider value is case-sensitive!
            if service.properties['provider'] == 'ArcObjects': # provider='ArcObjects' means the service is running under the ArcMap runtime i.e. published from ArcMap
                arcmapsvcs.append(service)
            elif service.properties['provider'] == 'ArcObjects11': # provider='ArcObjects11' means the service is running under the ArcGIS Pro runtime i.e. published from ArcGIS Pro
                prosvcs.append(service)
            elif service.properties['provider'] == 'DMaps': # provider='DMaps' means the service is running in the shared instance pool (and thus running under the ArcGIS Pro provider runtime)
                sharedinstancesvcs.append(service)
            else: pass # whoa nelly! unknown type of provider.. must be a fancy new server released after this script was written

    return (arcmapsvcs, prosvcs, sharedinstancesvcs)

args = parseInputParameters()

print('Connecting to %s..' % args.server)
import arcgis.gis.server
server = arcgis.gis.server.Server(args.server, username=args.user, password=args.password)

print("Connected, enumerating services..")
(arcmapsvcs, prosvcs, sharedinstancesvcs) = listServices(server)
print()

if len(arcmapsvcs) == 0:
    print('There are no services published from ArcMap. Impressive!')
else:
    print('Services from ArcMap not eligible to run in the shared instance pool:')
    for service in arcmapsvcs:
        print('- ' + service.properties['serviceName'])
        # imagine what uncommenting this would do..
        #if args.update:
        #   data = urllib.parse.urlencode({ 'token' : service._con.token, 'provider' : 'ArcObjects11', 'f' : 'json' }).encode('ascii')
        #   with urllib.request.urlopen(service.url + '/changeProvider', data) as f:
        #       print(f.read().decode('utf-8'))

print()
if len(prosvcs) == 0:
    print('There are no services published from ArcGIS Pro that are running with dedicated instances.')
else:
    print('Services from ArcGIS Pro eligible to run in the shared instance pool:')
    for service in prosvcs:
        print('- ' + service.properties['serviceName'])

        # switch Pro-based service to shared instance pool
        if args.update:
            # updating the provider can only be done via the dedicated changeProvider operation, it can't be  done by 
            # simply editing the service properties directly (backwards compatibility concession to avoid older 
            # ArcGIS Desktop clients from modifying this property incorrectly)
            data = urllib.parse.urlencode({ 'token' : service._con.token, 'provider' : 'DMaps', 'f' : 'json' }).encode('ascii')
            with urllib.request.urlopen(service.url + '/changeProvider', data) as f:
                print(f.read().decode('utf-8'))

print()

if len(sharedinstancesvcs) == 0:
    print('There are no services published from ArcGIS Pro that are running in the shared instance pool.')
else:
    print('Services from ArcGIS Pro already running in the shared instance pool:')
    for service in sharedinstancesvcs:
        print('- ' + service.properties['serviceName'])
