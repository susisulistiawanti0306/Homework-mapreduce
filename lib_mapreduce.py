#Nama : Susi Sulistiawanti
#HomeWork Mapreduce


from mrjob.job import MRJob #library Mrjob pada python
import re

WORD_RE = re.compile(r"[\w']+") #regerds buat cari work


class MRWordFreqCount(MRJob):
    # default mapper 
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1) 
            #fungsi sebagai literator (object python) bisa menyimpan in memory [(gunting,1),(headset,1), (kertas,1), (paku,1), (palu,1)]

    #reduce output dari mapper
    def reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
    #untuk menjalankan MRwordFreqCount
     MRWordFreqCount.run()
