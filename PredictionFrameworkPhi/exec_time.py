import xml.etree.ElementTree as ET
import os
import subprocess

    
def run():
    xml_file = "execution_data.xml"
    env = os.environ.copy()
    env['SINK_LD_LIBRARY_PATH'] = '/home/rcamacho'
    (app, affinity, cores, input_size, tpc) = get_variables(xml_file)
    threads = str(int(cores)*int(tpc))
    command = build_command(app, affinity, input_size, cores, tpc, threads)
    outputString = subprocess.check_output(command, env=env, shell=True)
    decodedString = outputString.decode('UTF-8')
    execTime = float(decodedString.rsplit(' ', 3)[1])
    
    return execTime

def build_command(app, affinity, inputSize, cores, tpc, threads):
    command = "micnativeloadex "
    if app.count("matrixMultiply") > 0:
        command += app
        command += " -a \""
        command += inputSize
        command += " "
        command += threads
        command += " 10 10\" -e \"LD_LIBRARY_PATH=/home/rcamacho KMP_PLACE_THREADS="
        command += cores
        command += "c,"
        command += tpc
        command += "t KMP_AFFINITY="
        command += affinity
        command += "\""
    return command
    
    
def get_variables(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    for application in root.iter('application'):
        app = application.text
    for affinities in root.iter('affinity'):
        affinity = affinities.text
    for core in root.iter('cores'):
        cores = core.text
    for inputs in root.iter('input_size'):
        inputSize = inputs.text
    for tpcs in root.iter('threads_per_core'):
        tpc = tpcs.text
        
    return app, affinity, cores, inputSize, tpc

run()