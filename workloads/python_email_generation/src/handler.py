import json
import sys
import random
import time
import os
import couchdb
from threading import Thread
from queue import Queue
from monitor import monitor_peak

interval = 0.02

def writeEmails(keynames, filenames):
    print ("\t writing key: %s "%(keynames))
    numFiles = len(keynames)

    response = ""
    for idx in range(numFiles):
        response = response + "\n" + filenames[idx]

    return response

def upload_stream_to_couchdb(db, doc_id, content, filename, content_type=None):
    try:
        db.put_attachment(
            doc=db[doc_id],
            content=content,
            filename=filename,
            content_type=content_type
        )
    except couchdb.http.ResourceConflict:
        pass

names = [ "Emma","Olivia","Ava","Isabella","Sophia","Charlotte","Mia","Amelia","Harper","Evelyn","Abigail","Emily","Elizabeth","Mila","Ella","Avery","Sofia","Camila","Aria","Scarlett","Victoria","Madison",
    "Luna","Grace","Chloe","Penelope","Layla","Riley","Zoey","Nora","Lily","Eleanor","Hannah","Lillian","Addison","Aubrey","Ellie","Stella","Natalie","Zoe","Leah","Hazel","Violet","Aurora","Savannah",
    "Audrey","Brooklyn","Bella","Claire","Skylar","Lucy","Paisley","Everly","Anna","Caroline","Nova","Genesis","Emilia","Kennedy","Samantha","Maya","Willow","Kinsley","Naomi","Aaliyah","Elena","Sarah",
    "Ariana","Allison","Gabriella","Alice","Madelyn","Cora","Ruby","Eva","Serenity","Autumn","Adeline","Hailey","Gianna","Valentina","Isla","Eliana","Quinn","Nevaeh","Ivy","Sadie","Piper","Lydia","Alexa",
    "Josephine","Emery","Julia","Delilah","Arianna","Vivian","Kaylee","Sophie","Brielle","Madeline","Liam","Noah","William","James","Oliver","Benjamin","Elijah","Lucas","Mason","Logan","Alexander","Ethan",
    "Jacob","Michael","Daniel","Henry","Jackson","Sebastian","Aiden","Matthew","Samuel","David","Joseph","Carter","Owen","Wyatt","John","Jack","Luke","Jayden","Dylan","Grayson","Levi","Issac","Gabriel",
    "Julian","Mateo","Anthony","Jaxon","Lincoln","Joshua","Christopher","Andrew","Theodore","Caleb","Ryan","Asher","Nathan","Thomas","Leo","Isaiah","Charles","Josiah","Hudson","Christian","Hunter","Connor",
    "Eli","Ezra","Aaron","Landon","Adrian","Jonathan","Nolan","Jeremiah","Easton","Elias","Colton","Cameron","Carson","Robert","Angel","Maverick","Nicholas","Dominic","Jaxson","Greyson","Adam","Ian","Austin",
    "Santiago","Jordan","Cooper","Brayden","Roman","Evan","Ezekiel","Xavier","Jose","Jace","Jameson","Leonardo","Bryson","Axel","Everett","Parker","Kayden","Miles","Sawyer","Jason" ,""]

def genEmailText(numChars):
    liBase = "Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature,     discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of 'de Finibus Bonorum et Malorum' (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, Lorem ipsum dolor sit amet.., comes from a line in section 1.10.32.The standard chunk of Lorem Ipsum used since the 1500s is reproduced below for those interested. Sections 1.10.32 and 1.10.33 from de Finibus Bonorum et Malorum by Cicero are also reproduced in their exact original form, accompa"    

    numNames = len(names)
    baseStrLen = len(liBase)
    useName = random.randint(0,numNames-1)
    emailPrefix = "Hi, "+str(names[useName])
    emailBody = ""

    perIterLen = 50
    numIters = (numChars//perIterLen)+1

    for curIter in range(numIters):
        start = random.randint(0,baseStrLen-perIterLen)
        genText = liBase[start:start+perIterLen]
        emailBody = str(emailBody)+str(genText)

    emailText = str(emailPrefix)+"\t"+str(emailBody)

    return emailText

def handler(args, context=None):
    q_cpu = Queue()
    q_mem = Queue()
    t = Thread(
        target=monitor_peak,
        args=(interval, q_cpu, q_mem),
        daemon=True
    )
    t.start()

    start = time.time()
    # bucketName = "s3trialsource"
    # prefixNum = random.randrange(1,999)
    # keyName = "t"+str(prefixNum)+".p0"
    # numEmails = 14000

    couch_link = args.get('couch_link')
    db_name = args.get('db_name')
    size = args.get("size")
    parallel = args.get("parallel")
    
    bucketName = "openwhisk"
    prefixNum = random.randrange(1,999)
    keyName = "t"+str(prefixNum)+".p0"
    numEmails = size
    numEmailsPerFile = 1500
    numFiles = numEmails//numEmailsPerFile
    totalFilesize = 1024*128
    maxFileSize = totalFilesize//numEmailsPerFile

    couch = couchdb.Server(couch_link)
    db = couch[db_name]

    readEnd = time.time()
    allText = []
  
    bucketStr = str(bucketName)+"\t prefix "+str(prefixNum)
    allEmails = ["hanfeiyu@uw.edu"] 
    genAccum = 0.0 
    writeAccum = 0.0 
    writeEnd = 0.0 
    opKey = [] 
    opFilenames = []
    opPrefixNum = random.randrange(1,9999)
    allFileStr = ""

    for curFileNum in range(1,numFiles+1):
        genStart = time.time()
        for curLine in allEmails:
            curEmail = curLine.strip()
            print ("\t curEmail: %s "%(curEmail))
            numChars = 500
            for curEmail in range(numEmailsPerFile):
                emailText = genEmailText(numChars)
                emailContent = str(curEmail)+"\n"+str(emailText)
                allText.append(emailContent)

        genEnd = time.time()
        genAccum = genAccum + (genEnd - genStart)

        writeStart = time.time()
        opFilenames = ["op.log"]
        opKey = ["op_"+str(opPrefixNum)+"_f"+str(curFileNum)+".log"]
        
        if(curFileNum==0): 
            bucketStr = str(bucketStr)+"\t opkey "+str(opKey[0])
            allFileStr = "AllFiles--> "+str(opKey[0])
        else:
            allFileStr = str(allFileStr)+"\t"+str(opKey[0])


        opFile = open(opFilenames[0],'w')
        print ("\t len(allText): %d len(allEmails): %d "%(len(allText),len(allEmails),))
        totLen = 0
        for emailIdx,curEmailText in enumerate(allText):
            opFile.write(str(curEmailText)+"\n")
            totLen = totLen + len(curEmailText)
            if(totLen>maxFileSize):
                print ("\t emailIdx: %d totLen: %d "%(emailIdx,totLen))
                break

        opFile.close()
        writeEnd = time.time()
        writeAccum = writeAccum + (writeEnd-writeStart)

    response = writeEmails(opKey, opFilenames)
    upload_stream_to_couchdb(db, "result", response.encode("utf-8"), "result.txt")
    end = time.time()
    timeTaken = end-start
    genTimeTaken = genAccum
    writeTimeTaken = writeAccum

    bucketStr = str(bucketStr)+"\t time: "+str('%.3f'%(timeTaken))+"\t genTimeTaken "+str('%.3f'%(genTimeTaken))+"\t writeTimeTaken "+str('%.3f'%(writeTimeTaken))+"\t end: "+str('%.3f'%(end))+"\t start "+str('%.3f'%(start))
    print ("\t allFiles: %s "%(allFileStr))
    print ("\t bucketStr: %s "%(bucketStr))

    cpu_timestamp = []
    cpu_usage = []
    while q_cpu.empty() is False:
        (timestamp, cpu) = q_cpu.get()
        cpu_timestamp.append(timestamp)
        cpu_usage.append(cpu)

    mem_timestamp = []
    mem_usage = []
    while q_mem.empty() is False:
        (timestamp, mem) = q_mem.get()
        mem_timestamp.append(timestamp)
        mem_usage.append(mem)

    return {
        "cpu_timestamp": [str(x) for x in cpu_timestamp],
        "cpu_usage": [str(x) for x in cpu_usage],
        "mem_timestamp": [str(x) for x in mem_timestamp],
        "mem_usage": [str(x) for x in mem_usage]
    }
    