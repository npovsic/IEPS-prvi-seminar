import binascii
import hashlib

# size of substring shingle
SHINGLE_SIZE = 10

class HashDriver:

    """
        Split  text to shingles of size SHINGLE_SIZE and output them as integers of fixed size
    """
    def text_to_shingle_set(self, text):
        words = text.split()

        # keeps word shingles
        shingles_in_doc_words = set()

        # keeps hashed shingles
        shingles_in_doc_ints = set()

        shingle = []

        shingle_size = SHINGLE_SIZE

        for index in range(len(words) - shingle_size + 1):
            shingle = words[index:index + shingle_size]
            shingle = ' '.join(shingle)

            # Hash the shingle to a 32-bit integer (create token)
            crc = binascii.crc32(shingle.encode()) & 0xffffffff

            if shingle not in shingles_in_doc_words:
                shingles_in_doc_words.add(shingle)

            if crc not in shingles_in_doc_ints:
                shingles_in_doc_ints.add(crc)

        return shingles_in_doc_ints

    def create_content_hash(self, html_content):
        try:
            m = hashlib.sha256()

            m.update(html_content.encode('utf-8'))

            return m.hexdigest()
        except Exception as error:
            print("     [CRAWLING] Error while creating content hash", error)

            return None
