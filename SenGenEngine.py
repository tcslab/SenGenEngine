import traceback
import sys
from ConfigParser import SafeConfigParser
import threading
import time
import requests
import warnings
import datetime



def compare_lists(a,b):
    unique_a = []
    unique_b = []
    for el in a:
        if el not in b:
            unique_a.append(el)
    for el in b:
        if b not in a:
            unique_b.append(el)
    return [unique_a,unique_b]

        
class SenGenEngine():
    
    def __init__(self):
        cfg = SafeConfigParser()
        cfg.read('config.ini')
        self.API_BASE_URI = cfg.get('settings','API_BASE_URI')
        try:
            self.rd_update_interval = float(requests.get(self.API_BASE_URI+"getSetting.php?name=rd_update_interval").json()['Settings'][0]['value'])
            self.dtset_update_interval = float(requests.get(self.API_BASE_URI+"getSetting.php?name=dtset_update_interval").json()['Settings'][0]['value']) 
            self.SYNDESI_BASE_URI = "http://" + requests.get(self.API_BASE_URI+"getSetting.php?name=ip").json()['Settings'][0]['value'] + ":" + requests.get(self.API_BASE_URI+"getSetting.php?name=port").json()['Settings'][0]['value']
        except:
            traceback.print_exc()
            print "Get settings from server failed. Exiting.."
            sys.exit()

    def start(self):
        rd_thread = threading.Thread(target=self.run_rd_thread)                  
        dtset_thread = threading.Thread(target=self.run_dtset_thread)
        rd_thread.start()
        print "Resource directory automatic update running..."
        dtset_thread.start()
        print "Dataset automatic update running..."
        
    def run_rd_thread(self):
        while True:
            try:
                self.update_rd()
                print datetime.datetime.now()
                print "    Resource directory updated"
            except:
                print datetime.datetime.now()
                warnings.warn("Resourse directory update failed")
                traceback.print_exc()
            time.sleep(self.rd_update_interval)

    
    def run_dtset_thread(self):
        while True:
            try:
                self.update_dtset()
                print datetime.datetime.now()
                print "    Dataset updated"
            except:
                print datetime.datetime.now()
                warnings.warn("Dataset update failed")
                traceback.print_exc()
            time.sleep(self.dtset_update_interval)
    
    def get_node_id_list(self,nodes,rd=False):
        ids = []
        if nodes :
            for node in nodes:
                if rd:
                    ids.append(node['name'])
                else:
                    ids.append(node['node_id'])
        return ids
    
    def get_nodes(self):
        nodes_list = []
        url = self.SYNDESI_BASE_URI + "/ero2proxy/service"
        get_nodes_response = requests.get(url)
        get_nodes_response.raise_for_status()
        try:
            nodes = get_nodes_response.json()["services"]
        except:
            nodes = {}
        for idx, val in enumerate(nodes):
                resource = val["resources"][0]
                node = {'hostname': resource["hostname"],
                                        # 'ip': resource["ip"],
                                        'ip': resource["uri"].replace("\\",""),
                                        'position' : [0, 0, 0],
                                        'port': resource["port"],
                                        'type': resource["type"],
                                        'protocol': resource["protocol"],
                                        'uri': resource["uri"].replace("\\",""),
                                        'hardware': resource["hardware"],
                                        'node_id': resource["node_id"], 'resources' : None}
                resourceval = []
                for i in range(0,len(val["resources"])):
                                resource = val["resources"][i]
                                resourceval.append({'name': resource["resourcesnode"]["name"],
                                            'path': resource["resourcesnode"]["path"],
                                            'data_type': resource["resourcesnode"]["data_type"],
                                            'type': resource["resourcesnode"]["type"]})
                                if resourceval[i]["type"][:8] == "ipso.sen":
                                    tstamp = {'timestamp' : resource["resourcesnode"]["timestamp"]}
                                    resourceval[i].update(tstamp)
                node.update({'resources': resourceval})
                nodes_list.append(node)

        result = []
        result.extend(nodes_list)
        return result

    def update_dtset(self):
        """query rd for node urls"""
        nodes = self.get_nodes()
        sensor_types = {'illuminance':'lux','temperature':'celsius','humidity':'%','actuator':'0-1'} 
        for sensor_type, unit in sensor_types.items():
			for node in nodes:
				for idx,resourcenode in enumerate(node['resources']):
					if resourcenode['type'][:8] == "ipso.sen":
						data = requests.get("http://"+node['ip']+":"+node['port']+resourcenode['path'])
						if sensor_type == 'actuator':
							try:
								update_actuation_state_response = requests.get(self.API_BASE_URI + "updateActuator1Status.php?name=" +node['node_id']+"&status=" +str(data.json['actuation_state']))
								update_actuation_state_response.raise_for_status()
							except:
								print datetime.datetime.now()
								warnings.warn("could not get " + sensor_type + " state from sensor" + resourcenode['path'][-6:])
								traceback.print_exc()	
						else:
							try:
								sen_value = data.json()[sensor_type]
								timestamp = data.json()['timestamp']
								"""TO FIX: insert positions in Syndesi an put real values in call"""
								insert_dtset_value_response = requests.get(self.API_BASE_URI + "insertValue.php?node_name=" +node['node_id']+ "&resource_name=" +sensor_type+ "+at+" +node['node_id']+ "&value=" +str(sen_value)+ "&unit=" +unit+ "&timestamp=" +resourcenode['timestamp']+"&pos_x=0&pos_y=0&pos_z=0")
								insert_dtset_value_response.raise_for_status()
								#print ("inserted " + node['node_id'] + " with " + sen_value)
							except:
								print datetime.datetime.now()
								warnings.warn("could not get " + sensor_type + " values from sensor" + resourcenode['path'][-6:])
								traceback.print_exc()
                        
        return 0
        
        return 0
    def update_rd(self):
        rd_get_nodes_response = requests.get(self.API_BASE_URI + "getNodes.php")
        rd_get_nodes_response.raise_for_status()
        try:
            rd_nodes = rd_get_nodes_response.json()['Nodes']
        except:
            rd_nodes = {}
        alive_nodes = self.get_nodes()
        diff = compare_lists(self.get_node_id_list(rd_nodes,True),self.get_node_id_list(alive_nodes))
        for idx, name in enumerate(diff[0]):
            delete_response = requests.get(self.API_BASE_URI + "delete.php?name="+name+"&type=nodes")
            delete_response.raise_for_status()
        for idx, name in enumerate(diff[1]):
            for node in alive_nodes:
                if name==node['node_id']:
                    pos_x = node['position'][0]
                    pos_y = node['position'][1]
                    pos_z = node['position'][2]
		    print (self.API_BASE_URI + "insertNode.php?name="+name+"&pos_x="+str(pos_x)+"&pos_y="+str(pos_y)+"&pos_z="+ str(pos_z))
		    print (self.API_BASE_URI + "getNodes.php?name="+name)
                    insert_node_response = requests.get(self.API_BASE_URI + "insertNode.php?name="+name+"&pos_x="+str(pos_x)+"&pos_y="+str(pos_y)+"&pos_z="+ str(pos_z))
                    insert_node_response.raise_for_status()
                    node_id_in_rd = requests.get(self.API_BASE_URI + "getNodes.php?name="+name).json()['Nodes'][0]['node_id']
                    for idx,resourcenode in enumerate(node['resources']):
                        if resourcenode['type'][:8] == "ipso.sen":
                            """TO FIX: put dynamic distribution of resource type when it's a sensor value"""
                            resource_type = 'Illuminance'      
                            resource_name = resourcenode['name'] + ' - Illuminance'
                        else:
                            resource_type = "Actuation " + resourcenode['name'].split()[-1]
                            resource_name = resourcenode['name']
                        type_id = requests.get(self.API_BASE_URI + "getTypes.php?name=" + resource_type).json()['Types'][0]['type_id']
                        insert_resource_response = requests.get(self.API_BASE_URI + "insertResource.php?name=" + resource_name + "&type_id=" + type_id + "&nodes_id=" + node_id_in_rd + "&path=" + resourcenode['path'].replace('&','|'))
                        insert_resource_response.raise_for_status()
        return 0
        

my_engine = SenGenEngine()
my_engine.start()


