import os
import struct
import threading
import time
from pathlib import PurePosixPath
from dotenv import load_dotenv
import smbclient
from smbprotocol.exceptions import SMBException
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

load_dotenv()

# H.265
HEVC_FORMATS = {"hvc1", "hev1", "hvc2", "hev2", "dvh1", "dvhe", "lhv1", "lhe1"}
# 中に子ボックスを持つボックス
CONTAINER_BOXES = {"moov", "trak", "mdia", "minf", "stbl"}

class Crawler:
    def __init__(self):
        self.smb_ip = os.getenv("SMB_IP")
        self.smb_path = os.getenv("SMB_PATH")
        self.smb_user = os.getenv("SMB_USER")
        self.smb_pass = os.getenv("SMB_PASS")
        self.max_workers = int(os.getenv("SMB_WORKERS", "16"))

        smbclient.ClientConfig(username=self.smb_user, password=self.smb_pass)
        self.share_path = PurePosixPath(f"\\{self.smb_ip}\{self.smb_path}")

        self.mp4_files = []
        self.lock = threading.Lock()

        self.local = threading.local()
        self.caches = []
        self.caches_lock = threading.Lock()

    def run(self):
        self.mp4_files = []
        try:
            self.crawl_mp4(self.share_path)
        finally:
            self.close_connections()

        return self.mp4_files

    def conn_kwargs(self):
        cache = getattr(self.local, "cache", None)
        if cache is None:
            cache = {}
            self.local.cache = cache
            with self.caches_lock:
                self.caches.append(cache)

        return {"connection_cache": cache}

    def close_connections(self):
        with self.caches_lock:
            caches, self.caches = self.caches, []

        for cache in caches:
            smbclient.reset_connection_cache(fail_on_error=False, connection_cache=cache)

        self.local = threading.local()

    @staticmethod
    def with_retry(func, what, attempts=5):
        for attempt in range(attempts):
            try:
                return func()

            except SMBException as e:
                if "credits are available" not in str(e) or attempt == attempts - 1:
                    raise

                print(f"Retrying {what} ({attempt + 1}/{attempts - 1})")
                time.sleep(0.1 * (2 ** attempt))

    def crawl_mp4(self, root_path):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            pending = {executor.submit(self.do_work, ("dir", root_path))}

            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    for item in future.result():
                        pending.add(executor.submit(self.do_work, item))

    def do_work(self, item):
        kind, path = item
        if kind == "dir":
            return self.scan_dir(path)

        return self.check_file(path)

    def check_file(self, path):
        try:
            if self.is_hevc(path):
                return []

        except Exception as e:
            print(f"Error probing {path}: {e}")

        with self.lock:
            self.mp4_files.append(path.relative_to(self.share_path))

        return []

    def scan_dir(self, current_path):
        work = []
        try:
            entries = self.with_retry(lambda: self.list_dir(current_path), f"scan {current_path}")
            for entry in entries:
                child_path = PurePosixPath(current_path) / entry.name

                if entry.is_dir():
                    work.append(("dir", child_path))

                elif entry.is_file():
                    if child_path.suffix.lower() == ".mp4":
                        work.append(("file", child_path))

        except Exception as e:
            print(f"Error accessing {current_path}: {e}")

        return work

    def list_dir(self, current_path):
        with smbclient.scandir(current_path, **self.conn_kwargs()) as entries:
            return list(entries)

    def is_hevc(self, path):
        formats = self.with_retry(lambda: self.read_sample_formats(path), f"probe {path}")
        return bool(formats & HEVC_FORMATS)

    def read_sample_formats(self, path):
        formats = set()
        with smbclient.open_file(path, mode="rb", **self.conn_kwargs()) as f:
            while True:
                header = f.read(8)
                if len(header) < 8:
                    break

                size, box_type = struct.unpack(">I4s", header)
                header_len = 8
                if size == 1:
                    ext = f.read(8)
                    if len(ext) < 8:
                        break
                    size = struct.unpack(">Q", ext)[0]
                    header_len = 16
                elif size == 0:
                    if box_type == b"moov":
                        self.collect_sample_formats(f.read(), formats)
                    break

                if size < header_len:
                    break

                if box_type == b"moov":
                    self.collect_sample_formats(f.read(size - header_len), formats)
                    break

                f.seek(size - header_len, os.SEEK_CUR)

        return formats

    def collect_sample_formats(self, data, formats):
        for box_type, payload in self.iter_boxes(data):
            if box_type in CONTAINER_BOXES:
                self.collect_sample_formats(payload, formats)

            elif box_type == "stsd":
                for entry_type, _ in self.iter_boxes(payload[8:]):
                    formats.add(entry_type)

    @staticmethod
    def iter_boxes(data):
        offset = 0
        while offset + 8 <= len(data):
            size, box_type = struct.unpack_from(">I4s", data, offset)
            header = 8
            if size == 1:
                if offset + 16 > len(data):
                    return
                size = struct.unpack_from(">Q", data, offset + 8)[0]
                header = 16
            elif size == 0:
                size = len(data) - offset

            if size < header:
                return

            yield box_type.decode("ascii", "replace"), data[offset + header:offset + size]
            offset += size