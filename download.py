from urllib.parse import urljoin
import zipfile
import requests
from zipfile import ZipFile
import gzip
import os, os.path
import glob
from bs4 import BeautifulSoup
import datetime
import argparse
import concurrent.futures
import config_upa

# parsing argumentu z prikazove radky
parser = argparse.ArgumentParser(description='Skript, ktery stahne vsechna data pro letosni rok z portalu CISJR')
parser.add_argument("-s", "--simulate", action="store_false", dest="download_files", help="pokud je nastaven, skript pouze prozkouma soubory na portale bez stazeni")

args = parser.parse_args()


# funkce, ktera stahne zadany soubor dle URL adresy
def downloadUrl(url, save_path, chunk_size=128):
    global FUNCCNT
    global TOTALFILES
    FUNCCNT += 1
    zipName = os.path.basename(url)
    print("Stahuji soubor ("+str(FUNCCNT)+"/"+TOTALFILES+"): " + zipName)
    r = requests.get(url, stream=True)
    print(save_path)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
    return zipName


# vstupni konfigurace pro skript
DOWNLOAD_FILES = args.download_files

FUNCCNT = 0

# pokud neexistuje nastavena slozka, skript ji vytvori
if not os.path.exists(config_upa.ZIPFOLDER):
    os.makedirs(config_upa.ZIPFOLDER)

# smazani obsahu slozky kvuli kolizim
#files = glob.glob(config_upa.ZIPFOLDER + '*')
#for f in files:
#    os.remove(f)

# soubor s hlavnimi JR, pridava se az na konec
mainDataZipUrl = 'https://portal.cisjr.cz/pub/draha/celostatni/szdc/2022/GVD2022.zip'

downloadList = []  # list pro seznam URL ke stazeni

folderUrlList = [] # list pro seznam URL adres mesicu k projiti a grabovani zip souboru

year = datetime.datetime.now().year
yearMonth = datetime.datetime.now().strftime("%Y-%m")

# vyhledani vsech slozek s jednotlivymi mesici daneho roku a uchovani jejich URL pro dalsi scraping
url = "https://portal.cisjr.cz/pub/draha/celostatni/szdc/"+str(year)+"/"
page = requests.get(url)
soup = BeautifulSoup(page.text, "html.parser")
for a in soup.find_all('a', href=True):
    if "[To Parent Directory]" in a:
        continue
    if str(year) not in a.text:
        continue
    if a['href'][-1] != "/":
        continue
    folderUrlList.append(urljoin(url, a['href']))

print("Prohledavane slozky na webu portalu CISJR:")
print('\n'.join('{}: {}'.format(*k) for k in enumerate(folderUrlList)))

# navstivime kazdou slozku a vybereme z ni vsechny soubory, ktere lze stahnout
for url in folderUrlList:
    cnt = 0
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")
    for a in soup.find_all('a', href=True):
        if "[To Parent Directory]" in a:
            continue
        if a['href'][-1] == "/":
            continue
        downloadList.append(urljoin(url, a['href']))
        cnt = cnt + 1
    print("Ve slozce "+str(url)+" bylo nalezeno " + str(cnt) + " souboru ke stazeni.")

# nakonec pridame nejvetsi soubor s hlavnimi daty
downloadList.append(mainDataZipUrl)
TOTALFILES = str(len(downloadList))
print("Celkovy pocet nalezenych souboru ke stazeni: " + TOTALFILES)


if not DOWNLOAD_FILES:
    exit("Skript byl ukoncen uspesne. Zadny soubor nebyl stazen.")

# pro rychlejsi stahovani vyuzijeme vice vlaken
with concurrent.futures.ThreadPoolExecutor() as executor:

    futures = []

    for url in downloadList:
        zipName = os.path.basename(url)
        futures.append(executor.submit(downloadUrl, url, config_upa.ZIPFOLDER+zipName))

    for future in concurrent.futures.as_completed(futures):
        if zipfile.is_zipfile(config_upa.ZIPFOLDER + future.result()):
            zipFileHandle = ZipFile(config_upa.ZIPFOLDER + future.result())
            zipFileHandle.extractall(path = config_upa.ZIPFOLDER)
            zipFileHandle.close()
        # jinak se pouzije gzip extrahovani obsahu
        else:
            f = gzip.open(config_upa.ZIPFOLDER + future.result(), 'r')
            file_content = f.read()
            file_content = file_content.decode('utf-8')
            f_out = open(config_upa.ZIPFOLDER + future.result()[:-4], 'w+', encoding="utf-8")
            f_out.write(file_content)
            f.close()
            f_out.close()

        #smazani puvodniho zipu aby nezabiral misto
        os.remove(config_upa.ZIPFOLDER + future.result())

exit("Skript byl ukoncen uspesne.")