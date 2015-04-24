import random
import xml.etree.ElementTree as ET

def run():
    xml_file = "execution_data.xml"
    affinity_list = ["\"balanced,granularity=fine\"", "\"balanced,granularity=core\"",
                     "\"compact,granularity=fine\"", "\"compact,granularity=core\"",
                     "\"scatter,granularity=fine\"", "\"scatter,granularity=core\""]
    new_value = random.randint(0, 5)
    tree = ET.parse(xml_file)
    root = tree.getroot()
    for affinity in root.iter('affinity'):
        affinity.text = str(affinity_list[new_value])
    tree.write(xml_file, xml_declaration=True)
    
    '''Add one to start list at 1 in DB '''
    return new_value + 1