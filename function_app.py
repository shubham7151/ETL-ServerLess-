import azure.functions as func
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO, BytesIO
import matplotlib.pyplot as plt
import json
import os
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="extractLoad")
def extractLoad(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    data = req.params.get('data')
    if (data == "threeThousand"):
            url='https://drive.google.com/file/d/1BepUNdnz7FTf5qPm4o-JmmLoV1KglkZd/view?usp=drive_link'
            url='https://drive.google.com/uc?id=' + url.split('/')[-2]
    else :
            url='https://drive.google.com/file/d/1KdQYN5_uEg2nQMjILh8iY-39ACTy_J0S/view?usp=sharing'
            url='https://drive.google.com/uc?id=' + url.split('/')[-2]
    df = pd.read_csv(url)
    output = df.to_csv(index=False, encoding="utf-8")
    blob_service_client = BlobServiceClient.from_connection_string("<<connectionString>>")
    container_client = blob_service_client.get_container_client("<<conatiner>>")
    blob_obj = blob_service_client.get_blob_client("<container>",blob="<blobName>")
    response  =blob_obj.upload_blob(output)
    if response :
        return func.HttpResponse(f"Hello, data is extraced from the source and loaded into Blob. You can access the ")


@app.blob_trigger(arg_name="myblob", path="logs/Rec.csv",
                               connection="AzureWebJobsStorage") 
def processingtrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    def savePlotToBLOB(plt,imageName,x, y):
        plt.figure(figsize=(15,10))
        plt.xticks(rotation=60)
        plt.title("Frequency count file extension")
        plt.bar(x,y)
        plt.xticks(rotation=90)
        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        blob_obj = blob_service_client.get_blob_client("logs",blob=imageName)
        blob_obj.upload_blob(buffer.read(),overwrite=True)
        buffer.close()
    def convertinMB(size):
        return size/1000
    def categorySize(size):
        if size <= 10:
            return "very small"
        if size > 10 and size <= 100 :
            return "small"
        if size > 100 and size <= 500 :
            return "medium"
        if size > 500 :
            return "large"
    def getPartialIp(ip):
        return ip[:-4]
    blob_service_client = BlobServiceClient.from_connection_string("<<ConnectionString>>")
    container_client = blob_service_client.get_container_client("<containerName>")
    blob_obj = blob_service_client.get_blob_client("<ContainerName>",blob="<blobName>")
    downloaded_blob = container_client.download_blob("<blobName>", encoding='utf8')

    df = pd.read_csv(StringIO(downloaded_blob.readall()), low_memory=False)
    ## save all values as json, with max min average for all values
    extensionCount = df.groupby("extention").count()
    extensionCount = extensionCount.sort_values(by="ip",ascending=False)[:5]['ip']
    file_type = extensionCount.to_json()
    savePlotToBLOB(plt,"extensionCount.png",extensionCount.index,extensionCount.values)
    df["code"]=df["code"].astype("int32").astype("str")
    codeCount = df.groupby("code").count()
    codeCount = codeCount.sort_values(by="ip",ascending=False)["ip"]
    code = codeCount.to_json()
    savePlotToBLOB(plt,"codeCount.png",codeCount.index,codeCount.values)
    df["sizeInMB"] = df["size"].apply(convertinMB)
    df["categorySize"]=df["sizeInMB"].apply(categorySize)
    categoryCount = df.groupby(by="categorySize").count()["ip"]
    category_count = categoryCount.to_json()
    savePlotToBLOB(plt,"categoryCount.png",categoryCount.index,categoryCount.values)
    countryReport = {"173.226.98": "United States", "52.21.42": "United States", "46.229.168": "United States", "66.185.161": "United States", "50.193.26": "United States", "146.88.97": "United States", "68.251.204": "United States", "38.108.201": "United States", "98.226.0": "United States", "115.111.60": "India", "65.92.86": "Canada", "50.35.79": "United States", "162.243.71": "United States", "147.108.222": "United States", "121.12.105": "China", "146.148.104": "United States", "38.94.157": "United States", "73.207.216": "United States", "117.245.32": "India", "69.196.253": "United States", "38.140.198": "United States", "174.111.39": "United States", "24.43.137": "United States", "96.67.27": "United States", "54.174.21": "United States", "61.12.94": "India", "216.86.50": "United States", "8.37.96": "United States", "198.182.154": "United States", "199.168.44": "United States", "111.93.160": "India", "198.20.253": "United States", "34.200.185": "United States", "192.223.243": "United States", "108.171.130": "United States", "68.192.50": "United States", "52.86.211": "United States", "5.9.145": "Germany", "70.183.20": "United States", "162.218.136": "United States", "54.82.22": "United States", "54.202.50": "United States", "66.19.225": "United States", "104.194.227": "United States", "54.175.208": "United States", "70.33.18": "United States", "144.230.191": "United States", "185.25.32": "United Kingdom", "52.73.130": "United States", "52.26.111": "United States", "69.191.211": "United States", "38.105.172": "United States", "173.82.141": "United States", "23.101.153": "United States", "198.134.52": "United States", "209.236.102": "United States", "162.218.138": "United States", "73.255.119": "United States", "72.67.52": "United States", "98.189.176": "United States", "192.193.171": "United States", "114.248.91": "China", "223.104.213": "China", "189.168.212": "Mexico", "98.207.156": "United States", "209.49.54": "United States", "121.40.65": "China", "85.118.133": "Czechia", "54.69.169": "United States", "66.249.64": "United States", "65.55.210": "United States", "107.23.191": "United States", "199.106.103": "United States", "216.228.233": "United States", "149.56.12": "Canada", "38.142.52": "United States", "68.226.154": "United States", "52.27.49": "United States", "51.255.65": "France", "117.91.231": "China", "120.26.124": "China", "187.227.99": "Mexico", "198.52.108": "United States", "54.67.64": "United States", "70.35.42": "United States", "210.51.244": "China", "67.202.213": "United States", "107.77.227": "United States", "34.230.36": "United States", "75.151.76": "United States", "76.116.110": "United States", "1.144.96": "Australia", "17.125.80": "United States", "45.32.128": "United States", "70.32.112": "United States", "99.126.178": "United States", "38.69.39": "United States", "75.105.72": "United States", "158.132.91": "Hong Kong", "209.249.4": "United States", "94.156.218": "Bulgaria", "69.18.47": "United States", "107.21.5": "United States", "188.163.72": "Ukraine", "167.181.12": "United States", "164.106.2": "United States", "134.159.131": "Australia", "74.121.38": "United States", "198.52.105": "United States", "121.58.175": "India", "199.190.211": "United States", "66.231.197": "United States", "117.61.138": "China", "74.83.83": "United States", "71.202.114": "United States", "98.116.86": "United States", "121.116.12": "Japan", "12.144.20": "United States", "111.93.214": "India", "17.133.3": "United States", "107.170.72": "United States", "216.87.243": "United States", "50.87.249": "United States", "203.190.151": "India", "45.55.8": "United States", "168.62.20": "United States", "128.250.0": "Australia", "165.117.232": "United States", "185.46.212": "Netherlands", "54.173.196": "United States", "52.200.13": "United States", "131.252.61": "United States", "54.162.220": "United States", "100.43.85": "United States", "199.67.131": "United States", "64.39.108": "United States", "199.184.253": "United States", "121.69.51": "China", "172.58.168": "United States", "45.59.248": "United States", "128.138.64": "United States", "24.147.73": "United States", "50.90.122": "United States", "99.129.194": "United States", "99.238.177": "Canada", "141.160.13": "United States", "192.48.239": "United States", "73.137.105": "United States", "198.45.18": "United States", "128.206.234": "United States", "54.172.238": "United States", "24.145.54": "United States", "173.164.151": "United States", "66.7.236": "United States", "163.172.71": "France", "173.254.28": "United States", "184.72.78": "United States", "69.191.249": "United States", "180.76.15": "China", "17.133.7": "United States", "54.202.221": "United States", "144.82.9": "United Kingdom", "207.58.180": "United States", "17.142.142": "United States", "173.227.38": "United States", "35.160.149": "United States", "159.255.167": "Iraq", "38.104.253": "United States", "122.96.62": "China", "129.217.232": "Germany", "92.40.248": "United Kingdom", "109.145.75": "United Kingdom", "67.225.243": "United States", "34.212.198": "United States", "202.155.246": "Hong Kong", "104.129.198": "United States", "116.226.185": "China", "72.47.244": "United States", "52.55.27": "United States", "45.18.9": "United States", "108.54.161": "United States", "108.91.91": "United States", "121.40.63": "China", "204.52.175": "United States", "107.77.211": "United States", "218.232.78": "South Korea", "210.175.208": "Japan", "42.61.184": "Singapore", "50.232.66": "United States", "34.201.25": "United States", "54.226.190": "United States", "52.39.165": "United States", "104.236.20": "United States", "54.173.181": "United States", "74.201.83": "United States", "107.22.225": "United States", "108.51.115": "United States", "112.64.217": "China", "107.21.18": "United States", "192.159.160": "United States", "47.19.127": "United States", "206.155.72": "United States", "121.40.158": "China", "78.41.194": "Russia", "108.171.135": "United States", "34.199.148": "United States", "204.155.229": "United States", "24.87.212": "Canada", "12.150.160": "United States", "106.120.173": "China", "88.99.27": "Germany", "50.59.18": "United States", "52.7.237": "United States", "121.40.162": "China", "192.99.55": "Canada", "67.225.139": "United States", "24.151.113": "United States", "12.247.105": "United States", "37.220.8": "United Kingdom", "173.251.67": "United States", "24.113.188": "United States", "209.170.214": "United States", "96.127.52": "United States", "184.106.197": "United States", "71.244.96": "United States", "85.10.208": "Germany", "35.165.139": "United States", "52.5.98": "United States", "185.20.6": "United Kingdom", "54.158.86": "United States", "104.226.4": "United States", "192.241.238": "United States", "186.80.60": "Colombia", "12.221.96": "United States", "139.199.159": "China", "38.96.141": "United States", "121.40.94": "China", "31.221.13": "United Kingdom", "125.16.1": "India", "185.182.48": "United Kingdom", "108.54.237": "United States", "52.52.228": "United States", "52.90.157": "United States", "73.24.98": "United States", "216.52.10": "United States", "17.133.6": "United States", "73.139.147": "United States", "69.16.221": "United States", "198.134.51": "United States", "174.47.193": "United States", "24.210.238": "United States", "192.223.136": "United States", "54.167.73": "United States", "72.30.14": "United States", "144.203.129": "United States", "66.249.66": "United States", "104.155.175": "United States", "107.13.237": "United States", "120.26.129": "China", "158.69.116": "Canada", "155.64.23": "United States", "52.90.9": "United States", "141.8.143": "United States", "192.193.216": "United States", "23.24.32": "United States", "74.208.74": "United States", "121.200.54": "India", "199.79.199": "United States", "107.142.44": "United States", "69.174.87": "United States", "208.184.140": "United States", "148.251.42": "Germany", "168.62.16": "United States", "184.95.45": "United States", "54.152.45": "United States", "184.172.134": "United States", "71.86.211": "United States", "129.107.80": "United States", "129.196.226": "United States", "64.160.34": "United States", "54.212.94": "United States", "70.113.10": "United States", "203.57.211": "Australia", "5.9.17": "Germany", "67.79.31": "United States", "5.9.112": "Germany", "49.77.227": "China", "13.90.101": "United States", "98.248.84": "United States", "52.89.16": "United States", "73.95.221": "United States", "38.88.175": "United States", "163.172.68": "France", "213.105.134": "United Kingdom", "34.227.92": "United States", "69.174.135": "United States", "34.229.147": "United States", "52.44.38": "United States", "17.133.2": "United States", "108.31.146": "United States", "52.73.13": "United States", "208.79.49": "United States", "216.14.33": "United States", "8.36.82": "United States", "157.55.39": "United States", "63.175.79": "United States", "167.219.0": "United States", "199.168.151": "United States", "69.128.227": "United States", "34.205.161": "United States", "69.162.16": "United States", "65.99.237": "United States", "54.145.53": "United States", "35.161.75": "United States", "54.89.107": "United States", "193.205.19": "Italy", "208.87.238": "United States", "38.117.200": "United States", "54.86.63": "United States", "95.111.49": "Bulgaria", "173.255.218": "United States", "46.12.130": "Greece", "99.229.102": "Canada", "64.209.89": "United States", "64.140.243": "United States", "45.218.215": "Morocco", "97.84.63": "United States", "34.209.60": "United States", "106.120.188": "China", "138.197.211": "United States", "12.12.171": "United States", "104.155.131": "United States", "163.117.2": "Spain", "38.100.171": "United States", "163.231.6": "United States", "96.83.83": "United States", "50.56.3": "United States", "108.28.83": "United States", "101.80.234": "China", "121.40.49": "China", "50.59.62": "United States", "199.30.24": "United States", "71.204.145": "United States", "188.165.200": "France", "213.229.84": "United Kingdom", "203.177.30": "Philippines", "70.90.69": "United States", "198.74.177": "United States", "192.185.4": "United States", "182.74.116": "India", "204.8.150": "United States", "54.165.81": "United States", "199.107.45": "United States", "205.197.242": "United States", "76.102.254": "United States", "52.201.223": "United States", "34.200.219": "United States", "192.64.159": "United States", "66.30.133": "United States", "162.138.210": "United States", "192.234.235": "United States", "38.140.108": "United States", "173.15.38": "United States", "73.222.244": "United States", "84.201.133": "Russia", "205.231.102": "United States", "66.81.238": "United States", "216.244.66": "United States", "54.91.91": "United States", "184.166.19": "United States", "199.76.117": "United States", "162.249.57": "United States", "64.124.25": "United States", "50.28.104": "United States", "54.84.62": "United States", "199.192.65": "United States", "24.5.117": "United States", "149.130.251": "United States", "169.196.173": "United States", "54.186.248": "United States", "131.253.25": "United States", "198.186.138": "United States", "34.203.38": "United States", "70.213.8": "United States", "98.210.214": "United States", "155.201.34": "United States", "219.140.61": "China", "71.190.137": "United States", "34.226.172": "United States", "123.116.144": "China", "128.199.163": "Singapore", "107.178.195": "United States", "54.152.30": "United States", "216.74.245": "United States", "98.3.20": "United States", "68.180.230": "United States", "221.194.44": "China", "54.197.165": "United States", "165.124.130": "United States", "209.237.237": "United States", "106.38.241": "China", "23.123.246": "United States", "101.81.133": "China", "34.209.20": "United States", "208.85.244": "United States", "40.77.167": "United States", "54.86.78": "United States", "164.132.159": "United Kingdom", "34.201.137": "United States", "68.23.253": "United States", "50.112.160": "United States", "27.154.114": "China", "38.97.87": "United States", "98.228.52": "United States", "38.105.116": "United States", "209.234.229": "United States", "104.133.2": "United States", "34.207.237": "United States", "162.27.66": "United States", "173.196.141": "United States", "62.65.39": "Estonia", "147.75.199": "Japan", "64.60.119": "United States", "183.128.143": "China", "115.113.198": "India", "52.91.247": "United States", "192.42.240": "United States", "137.135.107": "United States", "63.243.252": "United States", "216.59.114": "United States", "163.172.107": "France", "165.156.40": "United States", "199.207.253": "United States", "47.217.42": "United States", "38.122.108": "United States", "114.222.130": "China", "5.9.94": "Germany", "24.141.223": "Canada", "216.57.159": "United States", "38.125.122": "United States", "24.13.255": "United States", "52.23.159": "United States", "121.40.66": "China", "123.126.113": "China", "52.91.131": "United States", "204.131.227": "United States", "198.199.84": "United States", "50.116.50": "United States", "87.113.215": "United Kingdom", "13.94.212": "Netherlands", "34.200.232": "United States", "137.116.199": "Netherlands", "80.12.81": "France", "200.36.254": "Mexico", "174.47.155": "United States", "121.40.85": "China", "107.23.85": "United States", "167.219.88": "United States", "104.129.196": "United States", "198.231.9": "United States", "5.255.250": "United States", "204.101.161": "Canada", "121.40.161": "China", "216.145.126": "United States", "206.200.253": "United States", "64.57.81": "United States", "207.241.231": "United States", "12.249.145": "United States", "177.66.196": "Brazil", "65.52.133": "Netherlands", "4.30.121": "United States", "198.71.58": "United States", "54.76.49": "Ireland", "50.23.71": "United States", "121.40.120": "China", "18.85.22": "United States", "54.152.59": "United States", "109.68.190": "Russia", "77.88.47": "United States", "65.78.43": "United States", "108.175.11": "United States", "66.37.253": "United States", "218.30.103": "China", "198.52.97": "United States", "207.23.176": "Canada", "69.57.235": "Antigua and Barbuda", "35.167.212": "United States", "52.52.31": "United States"}
    df["partialIP"] = df["ip"].apply(getPartialIp)
    df['country'] = df['partialIP'].map(countryReport)
    countryRep = df.groupby("country").count()
    countryRep = countryRep.sort_values(by="ip",ascending=False)["ip"]
    country = countryRep.to_json()
    savePlotToBLOB(plt,"categoryCount.png",categoryCount.index,categoryCount.values)
    res = {'code':code, 'category':category_count, 'file type':file_type, "requestPerCounty":country}
    resultTOStore =json.dumps(res)
    blob_obj = blob_service_client.get_blob_client("logs",blob="<blobName>")
    blob_obj.upload_blob(resultTOStore)

    


@app.blob_trigger(arg_name="myblob", path="logs/result.json",
                               connection="AzureWebJobsStorage") 
def Nortification(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    PORT = 587  
    EMAIL_SERVER = "smtp-mail.outlook.com"  # Adjust server address, if you are not using @outlook

    # Read environment variables
    sender_email = "<email>"
    password_email = "<password>"

    blob_service_client = BlobServiceClient.from_connection_string("<<connectionString>>")
    container_client = blob_service_client.get_container_client("logs")
    blob_obj = blob_service_client.get_blob_client("logs",blob="result.json")
    downloaded_blob = json.loads(container_client.download_blob("result.json", encoding='utf8').readall())
    downloaded_blob = json.dumps(downloaded_blob)
    def send_email(subject, receiver_email, name, task, downloaded_blob):
    # Create the base text message.
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = formataddr(("MiltonETL", f"{sender_email}"))
        msg["To"] = receiver_email
        msg["BCC"] = sender_email

        msg.set_content(
            f"""\
            Dear {name},

            I hope this email finds you well.

            I am pleased to inform you that the Extract, Transform, Load (ETL) process for your {task} has been successfully completed. 

            Thank you for entrusting us with this important task. We look forward to continuing our partnership and delivering exceptional results.
--------------------------------------------------------------------------------
            DATA :: {downloaded_blob}

            Best regards
            MiltonETL
            """
        )
        # Add the html version.  This converts the message into a multipart/alternative
        # container, with the original text message as the first part and the new html
        # message as the second part.
        msg.add_alternative(
            f"""\
        <html>
        <body>
            <p>Hi {name},
            
            I hope this email finds you well.

            I am pleased to inform you that the process for your {task} has been successfully completed. 

            Thank you for entrusting us with this important task. We look forward to continuing our partnership and delivering exceptional results.

            <br>
            DATA :: {downloaded_blob}
            <br>
            Best regards
            MiltonETL</p>
        </body>
        </html>
        """,
            subtype="html",
        )

        with smtplib.SMTP(EMAIL_SERVER, PORT) as server:
            server.starttls()
            server.login(sender_email, password_email)
            server.sendmail(sender_email, receiver_email, msg.as_string())
    send_email("ETL Complete","<email>", "xyz", "server Logs",downloaded_blob)
    logging.info('emailComplete')