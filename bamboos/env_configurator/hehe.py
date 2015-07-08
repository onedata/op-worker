import json
import subprocess
import copy

def parse_json_file(path):
    """Parses a JSON file and returns a dict."""
    with open(path, 'r') as f:
        return json.load(f)


config = parse_json_file('env.json')
output = parse_json_file('output.json')


providers_map = {}
for provider_name in config['providers']:
    nodes = config['providers'][provider_name]['op_worker']['nodes'].keys()
    providers_map[provider_name] = {
        'nodes': []
    }
    for node in nodes:
        for worker_node in output['op_worker_nodes']:
            if worker_node.startswith(node):
                providers_map[provider_name]['nodes'].append(worker_node)
                providers_map[provider_name]['cookie'] = \
                    config['providers'][provider_name]['op_worker']['nodes'][node]['vm.args']['setcookie']

env_configurator_input = copy.deepcopy(config['global_setup'])
env_configurator_input['providers'] = providers_map

gr_node_name = config['globalregistry']['nodes'].keys()[0]
env_configurator_input['gr_cookie'] = config['globalregistry']['nodes'][gr_node_name]['vm.args']['setcookie']
env_configurator_input['gr_node'] = output['gr_nodes'][0]


print(json.dumps(env_configurator_input))

output = subprocess.check_output(['./env_configurator.escript', json.dumps(env_configurator_input)],
                        universal_newlines=True, stderr=subprocess.STDOUT)
print(output)
