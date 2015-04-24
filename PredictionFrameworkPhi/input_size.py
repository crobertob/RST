import random
import xml.etree.ElementTree as ET

def run():
    xml_file = "execution_data.xml"
    new_value = random.randrange(256, 6000, 256)
    tree = ET.parse(xml_file)
    root = tree.getroot()
    for core in root.iter('input_size'):
        core.text = str(new_value)
    tree.write(xml_file, xml_declaration=True)
    return new_value