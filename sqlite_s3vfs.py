import uuid
import io
import apsw
import hashlib
from math import log2
from boto3.s3.transfer import TransferConfig


class S3VFS(apsw.VFS):        
    def __init__(self, bucket, block_size=4096):
        self.name = f's3vfs-{str(uuid.uuid4())}'
        self._bucket = bucket
        self._block_size = block_size
        super().__init__(name=self.name, base='')

    def xAccess(self, pathname, flags):
        return (
            flags == apsw.mapping_access["SQLITE_ACCESS_EXISTS"]
            and any(self._bucket.objects.filter(Prefix=pathname + '/'))
        ) or (
            flags != apsw.mapping_access["SQLITE_ACCESS_EXISTS"]
        )

    def xFullPathname(self, filename):
        return filename

    def xDelete(self, filename, syncdir):
        self._bucket.objects.filter(Prefix=filename + '/').delete()

    def xOpen(self, name, flags):
        return S3VFSFile(name, flags, self._bucket, self._block_size)

    def serialize_iter(self, key_prefix):
        for obj in self._bucket.objects.filter(Prefix=key_prefix + '/'):
            yield from obj.get()['Body'].iter_chunks()

    def serialize_fileobj(self, key_prefix):
        chunk = b''
        offset = 0
        it = iter(self.serialize_iter(key_prefix))

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        class FileLikeObj:
            def read(self, n=-1):
                n = \
                    n if n != -1 else \
                    4294967294 * 65536  # max size of SQLite file
                return b''.join(up_to_iter(n))

        return FileLikeObj()

    def deserialize_iter(self, key_prefix, bytes_iter):
        chunk = b''
        offset = 0
        it = iter(bytes_iter)

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        def block_bytes_iter():
            while True:
                block = b''.join(up_to_iter(self._block_size))
                if not block:
                    break
                yield block

        for block, block_bytes in enumerate(block_bytes_iter()):
            self._bucket.Object(f'{key_prefix}/{block:010d}').put(Body=block_bytes)

    def _list_files(self):
        files = set()
        for obj in self._bucket.objects.filter(Prefix=''):
            #get the first path component and put to set first component / second component
            files.add(obj.key.split('/')[0] + '/' + obj.key.split('/')[1]) 
        return list(files)
    
    def _storage_diagnostic(self, page_size, block_size):
        report = {}
        for file in self._list_files():
            report[file] = []
            serialized = self.serialize_fileobj(file).read()
            #split the serialized byte array into chunks of page_size length
            chunks = [serialized[i:i+page_size] for i in range(0, len(serialized), page_size)]
            #for each chunk calculate the md5sum
            for chunk in chunks:
                #calculate md5sum
                md5sum = hashlib.md5(chunk).hexdigest()
                report[file].append(md5sum)
        return report
    
class S3VFSFile:
    def __init__(self, name, flags, bucket, block_size):
        self._key_prefix = \
            self._key_prefix = name.filename() if isinstance(name, apsw.URIFilename) else \
            name
        self._bucket = bucket
        self._block_size = block_size

    def _blocks(self, offset, amount):
        while amount > 0:
            block = offset // self._block_size  # which block to get
            start = offset % self._block_size   # place in block to start
            consume = min(self._block_size - start, amount)
            yield (block, start, consume)
            amount -= consume
            offset += consume

    def _get_block_bytes(self, block):
        try:
            with io.BytesIO() as f:
                self._bucket.download_fileobj(f'{self._key_prefix}/{block:010d}', f, Config = TransferConfig(use_threads = False)) 
                return f.getvalue()
        except Exception as e:
            return b''
        
    def _put_get_block_bytes(self, block, bytes):
        with io.BytesIO(bytes) as f:
            self._bucket.upload_fileobj(f, f'{self._key_prefix}/{block:010d}' , Config = TransferConfig(use_threads = False)) 

    def xRead(self, amount, offset):
        def _read():
            for block, start, consume in self._blocks(offset, amount):
                block_bytes = self._get_block_bytes(block)
                yield block_bytes[start:start+consume]

        return b"".join(_read())

    def xSectorSize(self):
        #defines the maximum atomically writable chunk
        #but sqlite can only align on powers of 2 > 512
        if self._block_size > 512 and log2(self._block_size) % 1 == 0:
            return self._block_size
        #otherwise we give up and accept unaligned writes
        return 0
    
    def xDeviceCharacteristics(self):
        #we can safely append and sequentially write
        flags = apsw.mapping_device_characteristics['SQLITE_IOCAP_SAFE_APPEND'] | apsw.mapping_device_characteristics['SQLITE_IOCAP_SEQUENTIAL']
        #set flags fropm 512 to 64k
        if self._block_size == 512:
            flags = flags | apsw.mapping_device_characteristics['SQLITE_IOCAP_ATOMIC512']
        if self._block_size == 1024:
            flags = flags | apsw.mapping_device_characteristics['SQLITE_IOCAP_ATOMIC1K']
        if self._block_size == 2048:
            flags = flags | apsw.mapping_device_characteristics['SQLITE_IOCAP_ATOMIC2K']
        if self._block_size == 4096:
            flags = flags | apsw.mapping_device_characteristics['SQLITE_IOCAP_ATOMIC4K']
        if self._block_size == 8192:
            flags = flags | apsw.mapping_device_characteristics['SQLITE_IOCAP_ATOMIC8K']
        if self._block_size == 16384:
            flags = flags | apsw.mapping_device_characteristics['SQLITE_IOCAP_ATOMIC16K']
        if self._block_size == 32768:
            flags = flags | apsw.mapping_device_characteristics['SQLITE_IOCAP_ATOMIC32K']
        if self._block_size == 65536:
            flags = flags | apsw.mapping_device_characteristics['SQLITE_IOCAP_ATOMIC64K']
            
        #our writes are networked so newer truly atomic, but we calculate the size from the stored object, and
        #sqlite can work with that guarantee for recovery purposes
        return flags
      
    def xFileControl(self, *args):
        return False

    def xCheckReservedLock(self):
        return False

    def xLock(self, level):
        pass

    def xUnlock(self, level):
        pass

    def xClose(self):
        pass

    def xFileSize(self):
        return sum(o.size for o in self._bucket.objects.filter(Prefix=self._key_prefix + "/"))

    def xSync(self, flags):
        return True

    def xTruncate(self, newsize):
        total = 0

        for obj in self._bucket.objects.filter(Prefix=self._key_prefix + "/"):
            total += obj.size
            to_keep = max(obj.size - total + newsize, 0)

            if to_keep == 0:
                obj.delete()
            elif to_keep < obj.size:
                obj.put(Body=obj.get()['Body'].read()[:to_keep])

        return True

    def xWrite(self, data, offset):
        lock_page_offset = 1073741824
        page_size = len(data)

        if offset == lock_page_offset + page_size:
            # Ensure the previous blocks have enough bytes for size calculations and serialization.
            # SQLite seems to always write pages sequentially, except that it skips the byte-lock
            # page, so we only check previous blocks if we know we're just after the byte-lock
            # page.

            data_first_block = offset // self._block_size
            lock_page_block = lock_page_offset // self._block_size
            for block in range(data_first_block - 1, lock_page_block - 1, -1):
                original_get_block_bytes = self._get_block_bytes(block)
                if len(original_get_block_bytes) == self._block_size:
                    break
                self._put_get_block_bytes(block, original_get_block_bytes + bytes(
                    self._block_size - len(original_get_block_bytes)
                ))

        data_offset = 0
        for block, start, write in self._blocks(offset, len(data)):

            data_to_write = data[data_offset:data_offset+write]

            if start != 0 or len(data_to_write) != self._block_size:
                original_get_block_bytes = self._get_block_bytes(block)
                original_get_block_bytes = original_get_block_bytes + bytes(max(start - len(original_get_block_bytes), 0))

                data_to_write = \
                    original_get_block_bytes[0:start] + \
                    data_to_write + \
                    original_get_block_bytes[start+write:]

            data_offset += write
            self._put_get_block_bytes(block, data_to_write)
