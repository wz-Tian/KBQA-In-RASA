from py2neo import Graph
from py2neo import NodeMatcher

graph = Graph("http://localhost:7474/browser/", auth=('xiaowuchanglu', 'zz0802212zz'), name='movieGraph')
nodematcher = NodeMatcher(graph)

print(graph.schema.node_labels)
print(graph.schema.relationship_types)
cypher = 'MATCH(n:movie{type:\'动作\', country:\'美国\'}) return n.name'
message_list = graph.run(cypher).data()
# for message in message_list:
#     print(message['n.name'])