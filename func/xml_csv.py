from xml.etree import ElementTree as ET
import pandas as pd
import logging
import requests
import zipfile
import os

def download_extract_zip(file,name:str,path: str=os.getcwd()):
    """_summary_
        Extract zip from xml
        unzip it
    Args:
        xml: xml file
        name(str): search for line
    """
    logging.warning("Getting download link")
    #Start parser
    xml_parse= ET.parse(file)
    #Go to root
    root=xml_parse.getroot()[1]
    #Get link
    for doc in root.findall("doc"):
        for child in doc:
            get_values=list(child.attrib.items())[0][1]
            if get_values == "download_link":
                if name in child.text:
                    output= child.text
        break
    #Start download
    logging.warning("Start download")
    download_start= requests.get(output,allow_redirects=True)
    #Create file 

    if download_start.ok:
        with open(f"{name}.zip",'wb') as f:
            f.write(download_start.content)
            logging.warning("File created")

    else:
        logging.warning("Error while downloading file")

    #unzip
    logging.warning("Extract file")
    path= path

    with zipfile.ZipFile(f"{path}\\{name}.zip", 'r') as zip_ref:
        zip_ref.extractall(path)
        return f"{name}.zip"

def xml_to_csv(xml,header):
    """_summary_
        Converts xml into a csv
    Args:
        xml (file): xml file
        header(list): a list of strings
    """
    #start parsing using ET
    logging.warning("Creating parser")
    xml_parse= ET.iterparse(xml,events=("start",))

    storage=[]#Storage for all information to csv dictionary list
    logging.warning("Going through parser")
    logging.warning("Creating list of tags and values")
    if "FinInstrmGnlAttrbts" and "Issr" in header:
        logging.warning("Going through lines in list")
        for event, values in xml_parse:
            if event =="start": #check start of the tags
                if "TermntdRcrd" in values.tag:# go to the req tag
            #Now we need to get every value in the xml file related to "FinInstrmGnlAttrbts" and "Issr" as this 
            #Create a comprehesion list 
                    list=[(value.tag,value) for value in values if "FinInstrmGnlAttrbts" in value.tag or "Issr" in value.tag]
            #Get every single value from list
                    dic={}#Store data in a dictiorary for a easy trasition to pd.dataframe
                    for tag, value in list:
                        if "FinInstrmGnlAttrbts" in tag: #First go through the childs of this value 
                            for filho in value:
                                if "Id" in filho.tag:
                                    dic[header[0]] = filho.text #ADD child to [0] key
                                elif "FullNm" in filho.tag:
                                    dic[header[1]] = filho.text #ADD child to [1] key
                                elif "ClssfctnTp" in filho.tag:
                                    dic[header[2]] = filho.text #ADD child to [2] key
                                elif "CmmdtyDerivInd" in filho.tag:
                                    dic[header[3]] = filho.text #ADD child to [3] key
                                elif "NtnlCcy" in filho.tag:
                                    dic[header[4]] = filho.text #ADD child to [4] key
                        else:#GO to Issr 
                            dic[header[5]]= filho.text #ADD child to [5] key(col)
                    storage.append(dic)
    else:
        raise ValueError("Header not apropriate, please change it to the required")
    logging.warning("All data stored")
    return storage

def create_dataset(storage: list,header: list):
    """_summary_
        Converts list of dictionary into an pd.Dataframe
    Args:
        storage(list): list of dictionary
        header(list): dataframe header
    """
    # creating initial dataframe
    #Merge each dataframe (append not working??)
    dataframe= pd.DataFrame(columns=header)
    storage= pd.DataFrame(storage)
    new_dataframe= pd.concat([dataframe,storage])
    final_dataframe=new_dataframe.to_csv("DLTINS.csv",index=False)
    logging.warning("CSV created")
    #Testing if new_dataframe is up
    if isinstance(new_dataframe, pd.DataFrame):
        return("DLTINS.csv")





if __name__ == "__main__":
    header= ["FinInstrmGnlAttrbts.Id","FinInstrmGnlAttrbts.FullNm","FinInstrmGnlAttrbts.ClssfctnTp",
    "FinInstrmGnlAttrbts.CmmdtyDerivInd","FinInstrmGnlAttrbts.NtnlCcy","Issr"]
    download_extract_zip("esma.xml","DLTINS")
    storage=xml_to_csv("DLTINS_20210117_01of01.xml",header)
    create_dataset(storage,header)


