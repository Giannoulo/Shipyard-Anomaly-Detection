
import pandas as pd

port_nodes = pd.read_csv('temp-nodes.csv', header=0, sep=',')
port_attributes = pd.read_csv('temp-attributes.csv', header=0, sep=',')

ports = port_nodes.set_index('shapeid').join(port_attributes.set_index('shapeid'))

ports = ports.iloc[:,[0,1,4]]

ports.to_csv('ports.csv', sep=',', header=True, index=False)