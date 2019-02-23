#!/usr/bin/env python3

###
SRC_FOLDER      = "../data"
CACHE_FOLDER    = "../cache"
DES_FOLDER      = "../bucket"

###
# /bucket/file_name
#   /text
#   /math
#   meta
#   sections
###


import subprocess
from subprocess import DEVNULL,STDOUT
import gzip
import shutil
import os
from pathlib import Path
import re
from lxml import etree
import pandas as pd
from glob import glob
import io
import tarfile

def tex_files(members):
    for tarinfo in members:
        if os.path.splitext(tarinfo.name)[1] == ".tex":
            yield tarinfo

def extract_tar_texfile(file,to=None):
    files = []
    with tarfile.open(file, "r:gz") as tar:
        for t in tar:
            if t.name.split('.')[1] =="tex":
                #files.append({"t.name" : tar.extractfile(t).read()})
                files.append(tar.extractfile(t).read())
    #for now use the first tex file
    to = file.split(".")[0] if to == None else to

    with open(to, "wb") as f:  f.write(files[0])
        
nss = {"l": "http://dlmf.nist.gov/LaTeXML"}

def clean(s):
    """
    remove digits and whitespace from section titles
    """
    s = re.sub(r'[-+]?\d*\.\d+|\d+', '', s)
    return s.strip()

def relevant_text(el):
    """
    probably need some logging - general document parse exception with doc id, element id etc.
    """
    def mk(e):return "" if e is None else e
    def _cond_(t):  return mk(t.text) + " "+ mk(t.tail) if t.tag in ["text"] else "##{}##".format(t.xpath("@xml:id"))
    txt =  (mk(el.text) + " ".join([_cond_(t) for t in el.getchildren() ]) +mk(el.tail)).replace("\n", " ")
    return txt.strip()


def gunzip_bytes_obj(bytes_obj):
    in_ = io.BytesIO()
    in_.write(bytes_obj)
    in_.seek(0)
    with gzip.GzipFile(fileobj=in_, mode='rb') as fo:
        gunzipped_bytes_obj = fo.read()

    return gunzipped_bytes_obj.decode()

def gz_to_xml(filename, cache_xml =True, remove_temp_file=False):
    print("processing" + filename)
    des_file = Path(filename).stem
    des_file = os.path.join(CACHE_FOLDER,des_file)
    
    #I am not sure of the rule at arxiv
    try:
        extract_tar_texfile(filename, des_file)
    except:
        with gzip.open(filename, 'rb') as f_in:
            with open(des_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    print("processing file "+des_file)

    command = ["latexml", des_file, "--destination="+des_file + ".xml"]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    #print("##########"+str(result.returncode)+" "+result.stderr )

    os.remove(des_file)

    #print(result.stderr # log and deal with failures TODO)
    
    print("###########PARSING DATA##########")
    data = etree.parse(des_file + ".xml")
    return data

def save_api_data(did):
    #exception setup - failed to call api
    #failed to save 
    from urllib.request import urlopen
    num_part =re.findall(r'\d+', did)[0]  
    url = """ http://export.arxiv.org/api/query?id_list={}""".format(did.replace(num_part, "/"+num_part))
    data = urlopen(url).read()
    with open(os.path.join(DES_FOLDER,did,"meta"), "wb") as r:  r.write(data)

def ensure_dir(dirname):
    if not os.path.exists(dirname):os.makedirs(dirname)

def save_text(data, name):
    recs = [{ #"text": n.text, 
               "data": relevant_text(n), 
               "id" : n.getparent().xpath("@xml:id")
              } for n in data.xpath(".//l:p", namespaces=nss)]

    dirname = os.path.join(DES_FOLDER,name,"text")
    ensure_dir(dirname)
    for r in recs:
        if r["id"] is None or len(r["id"]) < 1: continue
        with open(os.path.join(dirname, r["id"][0]), "w") as f:
            f.write(r["data"])

def save_math_data(data, name):
    recs = [{ #"text": n.text, 
               "data": n.xpath("@tex"), 
               "id" : n.xpath("@xml:id")
              } for n in data.xpath(".//l:Math",namespaces=nss)]

    dirname = os.path.join(DES_FOLDER,name,"math")
    ensure_dir(dirname)
    for r in recs:
        if r["id"] is None or len(r["id"]) < 1: continue
        if r["data"] is None or len(r["data"]) < 1: continue
        with open(os.path.join(dirname, r["id"][0]), "w") as f:
            f.write(r["data"][0])

def save_section_data(data, name):
    outname = os.path.join(DES_FOLDER,name,"section_meta")
    #get the csv or json data and save it

    sections = pd.DataFrame([{ #"text": n.text, 
               "data": clean(' '.join(n.itertext())), 
               "id" : n.getparent().xpath("@xml:id")
              } for n in data.xpath(".//l:title",namespaces=nss)])

    sections.to_csv(outname)

def proc_xml(data, name):
    name = Path(name).stem
    ensure_dir(os.path.join(DES_FOLDER,name))
    #get the api data and store it in the destination under the doc id
    print("Saving data for "+name)
    save_api_data(name)
    #for each p, save it by it's id
    save_text(data,name)
    #save the section data
    save_section_data(data,name)
    #save the math elements
    save_math_data(data,name)

#for all of files, do this
#file = "../data/9906/solv-int9906008.gz"

for f in list(Path(SRC_FOLDER).glob('**/*.gz'))[0:5]:
    f = str(f)
    try:
        #use the latexml to convert the unzipped src (assumed single tex) to subprocess mem result
        data = gz_to_xml(f)
        #take the xml data and process it
        proc_xml(data, Path(f).stem)
    except Exception as ex:
            print("###FAiLED ON#### "+f) + " because" + repr(ex)
            continue

print("DONE")