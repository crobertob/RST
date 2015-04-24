import random
import xml.etree.ElementTree as ET

def run():
    xml_file = "execution_data.xml"
    new_value = random.randint(1, 4)
    tree = ET.parse(xml_file)
    root = tree.getroot()
    for tpc in root.iter('threads_per_core'):
        tpc.text = str(new_value)
    tree.write(xml_file, xml_declaration=True)
    return new_value