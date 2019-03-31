import binascii
from random import randint

# minhash size
N = 128
max_val = (2 ** 32) - 1


class HashDriver:
    def __init__(self):
        # create N tuples that will serve as permutation functions
        # these permutation values are used to hash all input sets
        self.perms = [(randint(0, max_val), randint(0, max_val)) for i in range(N)]

        self.prime = 4294967311

    def text_to_shingle_set(self, text):
        words = text.split()

        # keeps word shingles
        shingles_in_doc_words = set()

        # keeps hashed shingles
        shingles_in_doc_ints = set()

        shingle = []

        shingle_size = 10

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

    def minhash(self, set):

        # initialize a sample minhash vector of length N
        vec = [float('inf') for i in range(N)]

        for val in set:

            # loop over each "permutation function"
            for perm_idx, perm_vals in enumerate(self.perms):
                a, b = perm_vals

                # pass 'val' through the 'ith' permutation function
                output = (a * val + b) % self.prime

                # conditionally update the 'ith' value of vec
                if vec[perm_idx] > output:
                    vec[perm_idx] = output

        # the returned vector repsresents the minimum hash of the set
        return vec
