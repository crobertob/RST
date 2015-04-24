import random
import xml.etree.ElementTree as ET

def run():
    xml_file = "execution_data.xml"
    #new_value = random.randint(0, 4)
    new_value = 0
    app_list = ["/home/rcamacho/matrixMultiply/phi/matrixMultiply.mic"]
    tree = ET.parse(xml_file)
    root = tree.getroot()
    for app in root.iter('application'):
        app.text = str(app_list[new_value])
    tree.write(xml_file, xml_declaration=True)
    return new_value + 1