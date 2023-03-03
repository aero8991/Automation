import os
import time
import pandas as pd
import shutil
import zipfile
import paramiko
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

files_from_job = r"\\w2prd-fil01\fileexchange\OakStreet\SureScripts\\"

#print(os.listdir(files_from_job))

zipped_files_output = r"\\w2prd-fil01\fileexchange\OakStreet\SureScripts\output\\"
archive_folder = r"\\w2prd-fil01\fileexchange\OakStreet\SureScripts\Archive\\"
received_file = r"\\w2prd-fil01\fileexchange\OakStreet\SureScripts\received_file\\"


time_to_wait = 10
time_counter = 0
while len(os.listdir(files_from_job)) < 9:
    time.sleep(1)
    time_counter += 1
    if time_counter > time_to_wait:break


#testing
#file_count.append("test_yes.txt")

def txt_files(source_dir):
    for filename in os.listdir(source_dir):
        if filename.endswith('.txt'):
            yield filename


def check_for_date(files):

    data = files.split("_")[1]
    trim_data = data[0:10]
    try:
        pd.to_datetime(trim_data)
    except Exception as e:
        print(e)
        return False

    return True

#checks that its a txt file, 
def valid_files(ssrs_path):
    file_count = []
    for path in os.listdir(ssrs_path):
        if os.path.isfile(os.path.join(ssrs_path, path)):
            if os.path.splitext(path)[1] == '.txt':
            
                
                if check_for_date(path):
                    file_count.append(path)
                else:
                    pass
    return file_count


clean_files = valid_files(files_from_job)

print(clean_files)


for txt_filename in os.listdir(files_from_job):
    if txt_filename.endswith('.txt'):
        
        name_without_extension = txt_filename[:-4]
        txt_path = os.path.join(files_from_job, txt_filename)
        zip_path = os.path.join(
            files_from_job, name_without_extension + '.zip')

        zip_file = zipfile.ZipFile(zip_path, 'w')
        zip_file.write(txt_path, txt_filename)
        zip_file.close()

for files in os.listdir(files_from_job):
    
    if files.endswith(".zip"):
        try:



            if os.path.isfile(zipped_files_output + '/'+files):
                os.remove(zipped_files_output + '/'+files)
                time.sleep(1)

            shutil.move(os.path.join(files_from_job, files),
                            zipped_files_output)
            time.sleep(3)

        except Exception as e:
            print(e)

    if files.endswith(".txt"):
        try:

            if os.path.isfile(archive_folder + '/'+files):
                os.remove(archive_folder + '/'+files)
                time.sleep(1)

            shutil.move(os.path.join(files_from_job, files),
                        archive_folder)
            time.sleep(3)



        except Exception as e:
            print(e)


### sftp connection
#SFTP connection info stored in Thycotic, replace .env variables
HOSTNAME=os.getenv('HOSTNAME')
USERNAME= os.getenv('USERNAME')
PASSWORD=os.getenv('PASSWORD')


#testing in pysftp
# cnopts = pysftp.CnOpts()
# cnopts.hostkeys = None

sftp_file_location = r"\\w2prd-fil01.genoa-qol.com\FILEEXCHANGE\testSai\SureScripts\\"

zip_files = os.listdir(zipped_files_output)


client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, allow_agent=False, look_for_keys=False)


sftp = client.open_sftp()
for file in zip_files:
    if file.endswith('.zip'):
        try:
            sftp.put(os.path.join(zipped_files_output, file),
                     '/SureScripts'  + "/" + file)
            print('uploaded file successfully: ', file)
            time.sleep(5)
        except Exception as e:
            print('failed to upload file: ', file, e)

            
        
## get file from SFTP SERVER

files = sftp.listdir('/SureScripts/test_file_location')
sftp.chdir('/SureScripts/test_file_location')
print(files)
    # for file in files:
for filename in sorted(sftp.listdir()):
    sftp.get(filename, received_file + filename)



### Move zip files to archive

for files in os.listdir(zipped_files_output):

        if files.endswith(".zip"):
            try:

                # if os.path.isfile(zipped_files_output + '/'+files):
                #     os.remove(zipped_files_output + '/'+files)
                #     time.sleep(1)

                shutil.move(os.path.join(zipped_files_output, files),
                            archive_folder)
                time.sleep(3)

            except Exception as e:
                print(e)



## delete old files 
for files in os.listdir(archive_folder):

    cutoff = datetime.now() - timedelta(minutes=5)
    mtime = datetime.fromtimestamp(os.path.getmtime(os.path.join(archive_folder, files)))

    if mtime < cutoff:
        print('File is more than 1 year old...')
        print("removing the file(s)...")
        try:
            os.remove(os.path.join(archive_folder, files))
            time.sleep(1)

        except Exception as e:
            print(e)
    else:
        print("false")
