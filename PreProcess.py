import sqlite3
import os
import time
import json


# Row class is the object of each row
class Row():
    def __init__(self,id=None, msno=None, city=None, bd=None, gender=None, registered_via=None, registration_init_time=None,
                 expiration_date=None, song_id=None, song_length=None, genre_ids=None, artist_name=None, composer=None,
                 lyricist=None, language=None, source_system_tab=None, source_type=None, source_screen_name=None,target=None,
                 name = None, isrc = None):

        self.id = id
        self.msno = msno
        self.city = city
        self.bd = bd
        self.gender = gender
        self.registered_via = registered_via
        self.registration_init_time = registration_init_time
        self.expiration_date = expiration_date
        self.song_id = song_id
        self.song_length = song_length
        self.genre_ids = genre_ids
        self.artist_name = artist_name
        self.composer = composer
        self.lyricist = lyricist
        self.language = language
        self.source_system_tab = source_system_tab
        self.source_type = source_type
        self.target = target
        self.source_screen_name = source_screen_name
        self.name = name
        self.isrc = isrc

        # class variables

        self._link_id = -1
        self.c = None
        self.conn = None
        self.script_dir = os.path.dirname(os.path.abspath(__file__))

    def __eq__(self, other):
        sid = self.id if self.id else 0
        oid = other.id if other.id else 0
        return sid == oid
    def __gt__(self, other):
        sid = self.id if self.id else 0
        oid = other.id if other.id else 0
        return sid > oid
    def __lt__(self, other):
        sid = self.id if self.id else 0
        oid = other.id if other.id else 0
        return sid < oid

    # connects to the DB, sets instance of cursor to class
    def __openDB(self):
        self.conn = sqlite3.connect(os.path.join(self.script_dir, 'data.db'))
        self.c = c = self.conn.cursor()
        return self

    # saves this item to the DB, for the first time (updateing a row should use the update method).
    # Links item to DB, future updates of this instance change only the selected row in the DB
    def save(self):
        if not self._link_id == -1:
            raise Exception('[+] Cannot save already linked row.  Perhaps try the update method')
        if not self.id:
            raise Exception('[+] Row must have an ID to save')
        if not self.conn:
            self.__openDB()
        # matching_ids = self.c.execute('SELECT * FROM Train WHERE id=?', self.id).fetchall()
        # if len(matching_ids) != 0:
        #     raise Exception('[+] Invalid ID, {} is already being used'.format(self.id))
        vals = tuple(
            [self.id, self.msno, self.city, self.bd, self.gender, self.registered_via, self.registration_init_time,
             self.expiration_date, self.song_id, self.song_length, self.genre_ids,self.artist_name,self.composer,
             self.lyricist, self.language, self.source_system_tab, self.source_type, self.target, self.source_screen_name, self.name, self.isrc]
        )
        self.c.execute('PRAGMA synchronous = OFF')
        self.c.execute('INSERT INTO Train VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',vals)
        self.conn.commit()
        self._link_id = self.id

    # Saves whatever the current state of each of the variables is in the database
    def update(self):
        if self._link_id == -1:
            raise Exception('[+] Cannot update un-linked row.  Perhaps try the save method')
        if not self.id:
            raise Exception('[+] Row must have an ID to update')
        if not self.conn:
            self.__openDB()
        vals = tuple(
            [self._link_id, self.msno, self.city, self.bd, self.gender, self.registered_via, self.registration_init_time,
             self.expiration_date, self.song_id, self.song_length, self.genre_ids, self.artist_name, self.composer,
             self.lyricist, self.language, self.source_system_tab, self.source_type, self.target, self.source_screen_name,
             self.name, self.isrc]
        )
        self.c.execute('UPDATE INTO Train VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', vals)
        self.conn.commit()

    # generator that yields rows that match the equality of the given arguments
    def load(self,msno=None,song_id=None, genre_ids=None, target=None):
        if not self.conn:
            self.__openDB()
        expr = ''
        vals = []
        if msno:
            expr += ' AND msno=?'
            vals.append(msno)
        if song_id:
            expr += ' AND song_id=?'
            vals.append(song_id)
        if genre_ids:
            expr += ' AND genre_ids=?'
            vals.append(genre_ids)
        if target:
            expr += ' AND target=?'
            vals.append(target)
        if not(msno and song_id and genre_ids and target):
           rows = self.c.execute('SELECT * FROM Train').fetchall()
        else:
            expr = 'SELECT * FROM Train WHERE ' + expr[4:]
            rows = self.c.execute(expr, tuple(vals)).fetchall()
        if rows == []:
            yield Row()
        else:
            for row in rows: # row is a tuple
                args = ['msno', 'city', 'bd', 'gender', 'registered_via', 'registration_init_time',
             'expiration_date', 'song_id', 'song_length', 'genre_ids', 'artist_name', 'composer',
             'lyricist', 'language', 'source_system_tab', 'source_type', 'target', 'source_screen_name',
             'name', 'isrc']
                yield Row(**dict(list(zip(args, row))))

    # close the DB connection if it exists
    def close(self):
        if self.conn:
            self.conn.close()
        return self

    # set the variables of this current Row
    def set(self,city=None, bd=None, gender=None, registered_via=None, registration_init_time=None,
                 expiration_date=None,song_length=None, genre_ids=None, artist_name=None, composer=None,
                 lyricist=None, language=None, source_system_tab=None, source_type=None, source_screen_name=None,target=None,
                 name = None, isrc = None):
        if city:
            self.city = city
        if bd:
            self.bd = bd
        if gender:
            self.gender = gender
        if registered_via:
            self.registered_via = registered_via
        if registration_init_time:
            self.registration_init_time = registration_init_time
        if expiration_date:
            self.expiration_date = expiration_date
        if song_length:
            self.song_length = song_length
        if genre_ids:
            self.genre_ids = genre_ids
        if artist_name:
            self.artist_name = artist_name
        if composer:
            self.composer = composer
        if lyricist:
            self.lyricist = lyricist
        if language:
            self.language = language
        if source_system_tab:
            self.source_system_tab = source_system_tab
        if source_type:
            self.source_type = source_type
        if target:
            self.target = target
        if source_screen_name:
            self.source_screen_name = source_screen_name
        if name:
            self.name = name
        if isrc:
            self.isrc = isrc

class Rows():
    def __init__(self, rows=None):
        self.rows = rows if rows != None else []
        self.c = None
        self.conn = None
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.csv_dir = os.path.join(self.script_dir, 'Data_CSV')

    # connects to the DB, sets instance of cursor to class
    def __openDB(self):
        self.conn = sqlite3.connect(os.path.join(self.script_dir, 'data.db'))
        self.conn.isolation_level = None # speed hack:  lets us begin and end beig data dumps + rollback
        self.c = self.conn.cursor()
        self.c.execute('PRAGMA synchronous = OFF')  # speed hack:  our id is already provided in data
        return self

    # create sqlite3 DB
    def initalize(self):
        self.conn = sqlite3.connect(os.path.join(self.script_dir,'data.db'))
        if not self.checkTableExists(self.conn, 'Train'):
            c = self.conn.cursor()
            c.execute('''CREATE TABLE Train
                         (id INTIGER PRIMARY KEY, msno text, city integer, bd integer, gender integer, registered_via integer,
                         registration_init_time integer, expiration_date integer, song_id text, song_length integer,
                         genre_ids text, artist_name text, composer text, lyricist text, language integer,
                         source_system_tab text, source_type text, target int, source_screen_name text, name text, isrc text)''')
            self.conn.commit()
        return self

    # check if table exists
    def checkTableExists(self, dbcon, tablename):
        dbcur = dbcon.cursor()
        try:
            dbcur.execute("SELECT * FROM {}".format(tablename))
            return True
        except:
            return False

    # move training data from CSV, returns generator of rows
    def loadFromCsv(self):
        with open(os.path.join(self.csv_dir, 'train.csv'), 'r') as file:
            print('[+] Opening Train File')
            d = file.readline()
            leng = len(d)
            print('[+] Row Count: {:,}'.format(leng))
            print('[+] First Row: {}'.format(d[1]))
            labels = d.replace('\n', '').split(',')
            index = 0
            for line in file:
                index += 1
                row = line.replace('\n', '').split(',')
                kwargs = dict(list(zip(labels, row)))
                yield Row(**kwargs, id=index)

    # update rows with song data.  Loads songs into memory
    # rows: generator (or list) of row objects
    #  returns generator of rows
    def songs(self, rows):
        with open(os.path.join(self.csv_dir, 'songs.csv'), 'r', errors='replace') as file:
            print('[+] Opening Song File')
            d = file.readlines()
            leng = len(d)
            print('[+] Row Count: {:,}'.format(leng))
            song_dict = {}
            start = time.time()
            i = 0
            for song in d:
                i += 1
                song = song.replace('\n', '').split(',')
                song_dict[song[0]] = song[1:]
                if i % 100000 == 0:
                    print('[+] loaded {} songs'.format(i))
            end = time.time()
            print('[+] pulled {} songs into memory in {} seconds'.format(leng, end - start))
            i, j = 0, 0
            for row in rows:
                i+=1
                try:
                    values = song_dict[row.song_id]
                except:
                    j += 1
                    values = [None, None, None, None, None, None]
                labels = ['song_length','genre_ids','artist_name','composer','lyricist','language']
                kwargs = dict(zip(labels,values))
                row.set(**kwargs)
                yield row
            print('[+] {} songs unrecognized, {}%'.format(j,(j*100)/i))

    # update rows with extra song info. Loads songs into memory
    # rows: generator (or list) of row objects
    #  returns generator of rows
    def songs_extra(self, rows):
        with open(os.path.join(self.csv_dir, 'song_extra_info.csv'), 'r', errors='replace') as file:
            print('[+] Opening Song_extra File')
            d = file.readlines()
            leng = len(d)
            print('[+] Row Count: {:,}'.format(leng))
            song_extra_dict = {}
            start = time.time()
            i = 0
            for song in d:
                i += 1
                song = song.replace('\n', '').split(',')
                song_extra_dict[song[0]] = song[1:]
                if i % 100000 == 0:
                    print('[+] loaded {} songs'.format(i))
            end = time.time()
            print('[+] pulled {} song_extras into memory in {} seconds'.format(leng, end - start))
            i, j = 0, 0
            for row in rows:
                i+=1
                try:
                    values = song_extra_dict[row.song_id]
                except:
                    j += 1
                    values = [None, None]
                labels = ['name', 'isrc']
                kwargs = dict(zip(labels, values))
                row.set(**kwargs)
                yield row

            print('[+] {} extra info songs unrecognized, {}%'.format(j, (j * 100) / i))

    # update rows with memvbers info. Loads members into memory
    # rows: generator (or list) of row objects
    #  returns generator of rows
    def members(self, rows):
        with open(os.path.join(self.csv_dir, 'members.csv'), 'r', errors='replace') as file:
            print('[+] Opening Members File')
            d = file.readlines()
            leng = len(d)
            print('[+] Row Count: {:,}'.format(leng))
            members_dict = {}
            start = time.time()
            i = 0
            for member in d:
                i += 1
                member = member.replace('\n', '').split(',')
                members_dict[member[0]] = member[1:]
                if i % 100000 == 0:
                    print('[+] loaded {} members'.format(i))
            end = time.time()
            print('[+] pulled {} members into memory in {} seconds'.format(leng, end - start))
            i, j = 0, 0
            for row in rows:
                i+=1
                try:
                    values = members_dict[row.msno]
                except:
                    j += 1
                    values = [None, None, None, None, None, None]
                labels = ['city', 'bd', 'gender', 'registered_via','registration_init_time', 'expiration_date']
                kwargs = dict(zip(labels, values))
                row.set(**kwargs)
                yield row

    # catagoricalize the rows for the given column name. Loads labels into memory
    # rows: generator (or list) of row objects
    # colname: string, name of column to catagorize
    # splitchar:  None if no split, ow split char (if multipile catagories are possibly represented)
    # returns generator of rows
    def catagoricalize(self, rows, colname, splitchar=None):
        exprs = {}
        exprs_inv = {}
        index = 0
        i = 0
        for row in rows:
            i += 1
            label = getattr(row, colname)
            if (not label) or (label == '0') or (label == 0):
                setattr(row, colname, '0')
            elif splitchar:
                labels = getattr(row, colname).split(splitchar)  # possibly many labels
                atr = []
                for label in labels:
                    try:
                        atr.append(exprs[label])
                    except:
                        index += 1
                        exprs[label] = str(index)
                        exprs_inv[str(index)] = label
                        atr.append(exprs[label])
                setattr(row, colname, splitchar.join(atr))
            else:
                try:
                    setattr(row, colname, exprs[label])
                except:
                    index += 1
                    exprs[label] = str(index)
                    exprs_inv[str(index)] = label
                    setattr(row, colname, index)
            yield row
        print('[+] catagorized {} rows with {} labels for column {}'.format(i, index, colname))
        inv_file = json.dumps(exprs_inv)
        with open(os.path.join(os.path.join(self.script_dir, 'Label_inv'), colname + '_inv.txt'), 'w') as file:
            file.write(inv_file)
        print('[+] dumped inverse labels for column {}'.format(colname))

    # update rows to replace None's with 0's
    # rows: generator (or list) of row objects
    #  returns generator of rows
    # NOTE:  if a col has been catagoricalized, it's Nones have already been replaced
    def fixNulls(self, rows):
        i = 0
        for row in rows:
            if not row.city:
                i+=1
                row.city = '0'
            if not row.bd:
                i += 1
                row.bd = '0'
            if not row.gender:
                i += 1
                row.gender = '0'
            if not row.registered_via:
                i += 1
                row.registered_via = '0'
            if not row.registration_init_time:
                i += 1
                row.registration_init_time = '0'
            if not row.expiration_date:
                i += 1
                row.expiration_date = '0'
            if not row.song_length:
                i += 1
                row.song_length = '0'
            if not row.genre_ids:
                i += 1
                row.genre_ids = '0'
            if not row.artist_name:
                i += 1
                row.artist_name = '0'
            if not row.composer:
                i += 1
                row.composer = '0'
            if not row.lyricist:
                i += 1
                row.lyricist = '0'
            if not row.language:
                i += 1
                row.language = '0'
            if not row.source_system_tab:
                i += 1
                row.source_system_tab = '0'
            if not row.source_type:
                i += 1
                row.source_type = '0'
            if not row.source_screen_name:
                i += 1
                row.source_screen_name = '0'
            if not row.name:
                i += 1
                row.name = '0'
            if not row.isrc:
                i += 1
                row.isrc = '0'
            yield row
        print('[+] fixed {} null results'.format(i))

    # saves all the rows in one delicous command.
    # takes generator of rows (rows argument)
    # loads no more then 100,000 rows into memory at once, to make data dumps in 100,000 piece chunks
    def saveall(self, rows):
        print('[+] Beggining saveall data dump')
        start = time.time()
        data = []
        index = 0
        if not (self.c):
            self.__openDB()
        s_old = time.time()
        for row in rows:
            index += 1
            if not row._link_id == -1:
                raise Exception('[+] Cannot save already linked row.  Perhaps try the update method')
            if not row.id:
                raise Exception('[+] Row must have an ID to save')
            if not row.msno:
                raise Exception('[+] Row has no MSNO, somewhere back this row has been invalidated')
            vals = tuple(
                [row.id, row.msno, row.city, row.bd, row.gender, row.registered_via, row.registration_init_time,
                 row.expiration_date, row.song_id, row.song_length, row.genre_ids,row.artist_name,row.composer,
                 row.lyricist, row.language, row.source_system_tab, row.source_type, row.target, row.source_screen_name,
                 row.name, row.isrc]
            )
            data.append(vals)
            if index % 100000 == 0:
                s_new = time.time()
                self.c.execute('PRAGMA synchronous = OFF')
                self.c.execute("begin")
                self.c.executemany('INSERT INTO Train VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', data)
                self.conn.commit()
                print('[+] Dumped {} values at {} seconds/row'.format(index, (s_new-s_old)/100000))
                s_old=s_new
                data = [] # 200000 chunks of data at a time
        end = time.time()
        print('[+] executed data dump of length {:,} in {} seconds'.format(index, end-start))

if __name__ == '__main__':
    print('begin')
    begin = time.time()
    M = Rows()
    M.initalize()
    g1 = M.loadFromCsv()
    g2 = M.songs(g1)
    g3 = M.songs_extra(g2)
    g4 = M.members(g3)
    g5 = M.fixNulls(g4)
    g6 = M.catagoricalize(g5,'source_system_tab')
    g7 = M.catagoricalize(g6, 'source_screen_name')
    g8 = M.catagoricalize(g7, 'source_type')
    g9 = M.catagoricalize(g8, 'name')
    g10 = M.catagoricalize(g9, 'isrc')
    g11 = M.catagoricalize(g10, 'lyricist', splitchar='|')
    g12 = M.catagoricalize(g11, 'composer', splitchar='|')
    g13 = M.catagoricalize(g12, 'artist_name', splitchar='|')
    g14 = M.catagoricalize(g13, 'gender')
    M.saveall(g14)
    thend = time.time()
    print("completed full pre-processing in {} seconds".format(thend-begin))


    # r = Rows()
    # print(len(list(r.load(song_id='BBzumQNXUHKdEBOB7mAJuzok+IJA1c2Ryg/yzTF6tik='))))
