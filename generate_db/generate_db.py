import sqlite3
import stat
import json
import hashlib
import zlib
from pathlib import Path
from typing import Callable, Any, Optional, Self

class crc32:
    digest_size = 4

    def __init__(self):
        self.state = 0

    def update(self, b: bytes):
        self.state = zlib.crc32(b, self.state)

    def digest(self) -> bytes:
        return self.state.to_bytes(4, "big")

BUFFER_SIZE = 1 << 18
MAX_QUEUE_SIZE = 1 << 10
STATE_DUMP_FILE = "tasks.json"
get_hasher = lambda : hashlib.md5(usedforsecurity = False)
DIGEST_LENGTH = get_hasher().digest_size

class DirData:
    def __init__(self):
        self.size = 0
        self.children: list[tuple[str, bool, int, bytes]] = []
        self.error = False

    def update(self, name: str, is_dir: bool, size: int, hash: bytes):
        self.size += size
        self.children.append((name, is_dir, size, hash))

    def get_hash(self) -> bytes:
        if self.error:
            return b"\0" * DIGEST_LENGTH
        hasher = get_hasher()
        hasher.update(f"{len(self.children)}:".encode("utf-8"))
        for name, is_dir, size, hash in sorted(self.children):
            if is_dir:
                hasher.update(b"d")
            hasher.update(f"{len(name)}:{name}".encode("utf-8"))
            size = str(size)
            hasher.update(f"{len(size)}:{size}".encode("utf-8"))
            hasher.update(hash)
        return hasher.digest()

    def to_json(self) -> dict[str, Any]:
        return {
            "size": self.size,
            "children": [
                [name, is_dir, size, hash.hex()] 
                for name, is_dir, size, hash in self.children
            ],
            "error": self.error
        }

    @classmethod
    def from_json(cls, state: dict[str, Any]) -> Self:
        result = cls()
        result.size = state["size"]
        result.children = [
            (name, is_dir, size, bytes.fromhex(hash))
            for name, is_dir, size, hash in state["children"]
        ]
        result.error = state["error"]
        return result
    
class Task:
    def run(self, manager):
        raise NotImplementedError()
    
    def to_json(self) -> tuple[str, Callable[[int], dict[str, Any]], DirData]:
        raise NotImplementedError()

    @classmethod
    def from_json(cls, entry: dict[str, Any]) -> tuple[Callable[[...], Self], list[int]]:
        match entry["type"]:
            case "scan_task":
                return ScanTask.from_json(entry["state"])
            case "finish_task":
                return FinishTask.from_json(entry["state"])

class Manager:
    def __init__(self,
        tasks: list[Task],
        con: sqlite3.Connection,
        verbose: bool
    ):
        self.tasks = tasks
        self.con = con
        self.verbose = verbose
        self.cur = con.cursor()
        self.queue: list[tuple[int, int, str, int, bytes]] = []
        self.current_task: Optional[Task] = None

    @classmethod
    def from_json(cls,
        state: dict[str, list],
        con: sqlite3.Connection,
        verbose: bool
    ) -> Self:
        datas = [DirData.from_json(data) for data in state["data"]]
        tasks = []
        for task_entry in state["tasks"]:
            create_task, data_ids = Task.from_json(task_entry)
            tasks.append(create_task(*(None if id == None else datas[id] for id in data_ids)))
        return cls(tasks, con, verbose)

    def run_tasks(self):
        while self.tasks:
            self.current_task = self.tasks.pop()
            self.current_task.run(self)
            self.current_task = None
            

    def push(self, task: Task):
        self.tasks.append(task)

    def insert_dir(self, parent: int, mode: int, name: str) -> int:
        self.cur.execute("INSERT INTO filesystem(parent, mode, name) VALUES(?, ?, ?)", (parent, mode, name))
        self.con.commit()
        return self.cur.lastrowid

    def insert_file(self, parent: int, mode: int, name: str, size: int, hash: bytes):
        if len(self.queue) == MAX_QUEUE_SIZE:
            self.execute_deferred()
        self.queue.append((parent, mode, name, size, hash))

    def execute_deferred(self):
        if self.verbose:
            print(f"Inserting {len(self.queue)} queued files")
        self.cur.executemany("INSERT INTO filesystem VALUES(?, ?, ?, ?, ?)", self.queue)
        self.con.commit()
        self.queue.clear()

    def update(self, parent: int, size: int, hash: bytes):
        self.cur.execute("UPDATE filesystem SET size = ?, hash = ? WHERE rowid = ?", (size, hash, parent))
        self.con.commit()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.execute_deferred()
        self.cur.close()
        if self.current_task:
            self.push(self.current_task)
        if self.tasks:
            state = {
                "data": [],
                "tasks": []
            }
            data_ids = []
            for task in self.tasks:
                type, get_state, datas = task.to_json()
                task_data_ids = []
                for data in datas:
                    if data == None:
                        task_data_ids.append(None)
                    elif id(data) in data_ids:
                        task_data_ids.append(data_ids.index(id(data)))   
                    else:
                        task_data_ids.append(len(data_ids))
                        data_ids.append(id(data))
                        state["data"].append(data.to_json())
                state["tasks"].append({
                    "type": type,
                    "state": get_state(*task_data_ids)
                })
            with open(STATE_DUMP_FILE, "w") as file:
                json.dump(state, file)

class FinishTask(Task):
    def __init__(self,
        id: int,
        name: str,
        data: DirData,
        parent_data: DirData
    ):
        self.id = id
        self.name = name
        self.data = data
        self.parent_data = parent_data

    def run(self, manager: Manager):
        hash = self.data.get_hash()
        manager.execute_deferred()
        if manager.verbose:
            print(f"Updating {self.id}")
        manager.update(self.id, self.data.size, hash)
        self.parent_data.update(self.name, True, self.data.size, hash)

    def to_json(self) -> tuple[str, Callable[[int, int], dict[str, Any]], list[DirData]]:
        return (
            "finish_task",
            lambda data_id, parent_data_id : {
                "id": self.id,
                "name": self.name,
                "data": data_id,
                "parent_data": parent_data_id,
            },
            [self.data, self.parent_data]
        )
    
    @classmethod
    def from_json(cls, state: dict[str, Any]) -> tuple[Callable[[DirData, DirData], Self], list[int]]:
        return (
            lambda data, parent_data : cls(state["id"], state["name"], data, parent_data),
            [state["data"], state["parent_data"]]
        )

class ScanTask(Task):
    def __init__(self,
        path: Path,
        parent: Optional[int],
        parent_data: DirData
    ):
        self.path = path
        self.parent = parent
        self.parent_data = parent_data

    def run(self, manager: Manager):
        result = self.path.lstat()
        if stat.S_ISDIR(result.st_mode):
            manager.execute_deferred()
            if manager.verbose:
                print(f"Inserting directory {self.path}")
            id = manager.insert_dir(self.parent, result.st_mode, self.path.name)
            data = DirData()
            manager.push(FinishTask(id, self.path.name, data, self.parent_data))
            try:
                for child in self.path.iterdir():
                    manager.push(ScanTask(child, id, data))
            except PermissionError:
                print(f"Not permitted to open directory {self.path}")
                data.error = True
        else:
            if stat.S_ISLNK(result.st_mode):
                hasher = get_hasher()
                hasher.update(str(self.path.readlink()).encode("utf-8"))
                hash = hasher.digest()
            else:
                if result.st_size == 4096 and self.path.name.startswith("._") and stat.S_ISREG(result.st_mode):
                    return
                if manager.verbose:
                    print(f"Scanning file {self.path} (size = {result.st_size})")
                with self.path.open("rb") as file:
                    hash = hashlib.file_digest(file, get_hasher).digest()
            if manager.verbose:
                print(f"Queueing file {self.path}")
            manager.insert_file(self.parent, result.st_mode, self.path.name, result.st_size, hash)
            self.parent_data.update(self.path.name, False, result.st_size, hash)

    def to_json(self) -> tuple[str, Callable[[int], dict[str, Any]], list[DirData]]:
        return (
            "scan_task",
            lambda parent_data_id : {
                "path": str(self.path),
                "parent": self.parent,
                "parent_data": parent_data_id,
            },
            [self.parent_data]
        )

    @classmethod
    def from_json(cls, state: dict[str, Any]) -> tuple[Callable[[DirData], Self], list[int]]:
        return (
            lambda parent_data : cls(Path(state["path"]), state["parent"], parent_data),
            [state["parent_data"]]
        )
                

def generate_db(path: str, database: str, verbose: bool, restore: Optional[str]):
    con = sqlite3.connect(database)

    if restore:
        with open(restore, "r") as file:
            with Manager.from_json(json.load(file), con, verbose) as manager:
                manager.run_tasks()
    else:
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS filesystem")
        cur.execute("CREATE TABLE filesystem(parent INTEGER, mode INTEGER, name TEXT, size INTEGER, hash BLOB)")
        con.commit()
        cur.close()
        with Manager([ScanTask(Path(path), None, DirData())], con, verbose) as manager:
            manager.run_tasks()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--database", default = "filesystem.db")
    parser.add_argument("--verbose", action = "store_true")
    parser.add_argument("--restore", default = None)
    parser.add_argument("--hash-type", default = "md5")
    args = parser.parse_args()
    generate_db(args.path, args.database, args.verbose, args.restore)