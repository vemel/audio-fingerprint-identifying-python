import sys
from abc import abstractmethod, ABC


class Database(ABC):
    TABLE_SONGS = None
    TABLE_FINGERPRINTS = None

    def __init__(self, a):
        self.a = a

    def connect(self):
        pass

    @abstractmethod
    def insert(self, table, params):
        raise NotImplementedError()

    @abstractmethod
    def insertMany(self, table, keys, values):
        raise NotImplementedError()

    @abstractmethod
    def findOne(self, table, params):
        raise NotImplementedError()

    def get_song_by_filehash(self, filehash):
        return self.findOne(self.TABLE_SONGS, {"filehash": filehash})

    def get_song_by_id(self, id):
        return self.findOne(self.TABLE_SONGS, {"id": id})

    def add_song(self, filename, filehash):
        song = self.get_song_by_filehash(filehash)

        if not song:
            song_id = self.insert(
                self.TABLE_SONGS, {"name": filename, "filehash": filehash}
            )
        else:
            song_id = song[0]

        return song_id

    def get_song_hashes_count(self, song_id):
        pass

    def store_fingerprints(self, values):
        self.insertMany(self.TABLE_FINGERPRINTS, ["song_fk", "hash", "offset"], values)
