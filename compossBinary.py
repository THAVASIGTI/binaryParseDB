import os,sys,sqlite3,logging,argparse,time

class binaryPush:
    def __init__(self,args):
        self.collectDB = args.DataBase
        if not os.path.isfile(args.File):
            self.binaryFileDecripts = list()
            for root,dirt,folder in os.walk(args.File):
                for files in folder:
                    fFileDest = os.path.join(root,files)
                    self.binaryFileDecripts.append(fFileDest)
        else:
            self.binaryFileDecripts = list(args.File)

    def reader(self,binaryFileDecript):
        size = os.stat(binaryFileDecript).st_size
        with open(binaryFileDecript,"rb") as f:
            content = f.read()
        return content,size

    def memory(self):
        conn = sqlite3.connect(self.collectDB)
        conn.execute("create table IF NOT EXISTS wraper(id INTEGER PRIMARY KEY, fName varchar,fBinary BLOB)")
        cursor = conn.cursor()
        return conn,cursor

    def push(self):
        logging.info('check Memober...')
        conn,cursor = self.memory()
        for binaryFileDecript in self.binaryFileDecripts:
            logging.info('check File...')
            fName = os.path.basename(binaryFileDecript)
            logging.info('compailing :'+str(fName))
            content,size = self.reader(binaryFileDecript)
            logging.info('size (Bi) :'+str(size))
            logging.info('writing..')
            try:
                cursor.execute("INSERT OR REPLACE INTO wraper (fName, fBinary) VALUES (?,?)",(fName,sqlite3.Binary(content)))
            except (sqlite3.IntegrityError,Exception) as e:
                logging.error(e)
        conn.commit()
        conn.close()
        time.sleep(4)

class binaryPull:
    def __init__(self,args):
        self.collectDB = args.DataBase
        self.destPath = args.DestPath

    def fetch(self,mode=str,no=int,to=int):
        conn = sqlite3.connect(self.collectDB)
        cursor = conn.cursor()
        if 'option' == mode:
            data = cursor.execute("select id, fName from wraper").fetchall()
            logging.info("list of binary files\n")
            for daata in data:
                print(str(daata[0])+" "+str(daata[1]))
                count = int(daata[0])
            ch = str(input("\nplazz enter only int type class. \n show option in roll num.\n=> "))
            return ch,count
        elif 'conn' == mode:
            print(no)
            if "*" == no:
                for no in range(1,to):
                    data = cursor.execute("select fName, fBinary from wraper where id = {0}".format(no)).fetchall()
                    print("pulling "+str(data[0][0]))
                    path = os.path.join(*[self.destPath,data[0][0]])
                    with open(path,"wb") as f:
                        f.write(data[0][1])
                    print('complited')
            else:
                data = cursor.execute("select fName, fBinary from wraper where id = {0}".format(no)).fetchall()
                print("pulling "+str(data[0][0]))
                path = os.path.join(*[self.destPath,data[0][0]])
                with open(path,"wb") as f:
                    f.write(data[0][1])
                print('complited')

    def pull(self):
        chNo,count = self.fetch(mode="option")
        self.fetch(mode='conn',no=chNo,to=count)

if __name__ == "__main__":
    DownPath = os.path.join(*[os.getcwd(),'lib'])
    DataBase = os.path.join(*[os.getcwd(),'store.db'])
    parser = argparse.ArgumentParser()
    parser.add_argument('-db','--DataBase',help="connect to db path new | old",default=DataBase)
    parser.add_argument('-f','--File',help="append DB in binary file",required=False)
    parser.add_argument('-d','--DestPath',help="Write local Path",required=False,default=DownPath)
    parser.add_argument('action',action='store',help="push | pull")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    # if not os.path.isfile(args.DataBase):
    #     logging.warning('not found DB.')
    #     sys.exit(1)

    if args.action == 'pull':
        os.makedirs(args.DestPath) if not os.path.isdir(args.DestPath) else logging.info('Dowload alredy placed')
        binaryPull(args).pull()

    if args.action == 'push' and not None is args.File:
        binaryPush(args).push()
    else:
        logging.warning('Puts Src File Dest.')
        sys.exit(1)