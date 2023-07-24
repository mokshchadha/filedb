import 'dart:convert';
import 'dart:io';

import 'package:basics/basics.dart';
import 'package:quiver/cache.dart';

enum RecordEventType { create, modify, delete }

class RecordEvent {
  final String key;
  final RecordEventType type;

  RecordEvent({required this.key, required this.type});
}

abstract class Record<T> {
  static final String fieldSeparator = "|";
  final String key;

  Record(this.key);

  set key(String newId) {}
}

abstract class RecordCollection<T extends Record> {
  late final Directory _storageDirectory;
  late final RecordStore _storage;
  late final int _cacheSize;
  late MapCache<String, List<int>> _cache;
  late String Function(T) partitionKey;

  RecordCollection(
      {required Directory storageDirectory,
      required this.partitionKey,
      cacheSize = 100}) {
    _storageDirectory = storageDirectory;
    _cacheSize = (cacheSize > 100 && cacheSize <= 1000000) ? cacheSize : 100;
    _cache = MapCache.lru(maximumSize: _cacheSize); // min 100 max 1M
  }

  String serialize(T record);

  T deserialize(String data);

  bool validate(T record);

  String _createPartitionKey(T record) {
    String pkey = partitionKey(record);
    if (pkey.isBlank || pkey.length > 255) {
      throw FormatException(
          "Partition key label cannot be blank or greater 255 characters.");
    }
    return "$pkey/${record.key}";
  }

  Future<bool> initialize() async {
    _storage = RecordStore(directory: _storageDirectory);
    await _storage.destroy();
    await _storage.initialize();
    return true;
  }

  Stream<String> get keys {
    return _storage.keys;
  }

  Future<T?> find(String key) async {
    List<int>? recordBytes = await _cache.get(key);
    if (recordBytes != null) {
      return deserialize(utf8.decode(recordBytes));
    }

    recordBytes = await _storage.find(key);
    if (recordBytes != null) {
      await _cache.set(key, recordBytes);
      T record = deserialize(utf8.decode(recordBytes));
      if (!validate(record)) {
        throw FormatException(
            "Record validation failed while fetching from disk.");
      }
      return record;
    }

    return Future.value(null);
  }

  Future<bool> insert(T record) async {
    if (!validate(record)) {
      throw FormatException("Record validation failed while insertion.");
    }
    return await _storage.insert(
        _createPartitionKey(record), utf8.encode(serialize(record)));
  }

  Future<bool> update(T record) async {
    if (!validate(record)) {
      throw FormatException("Record validation failed while update.");
    }

    List<int> recordBytes = utf8.encode(serialize(record));
    bool isUpdated =
        await _storage.update(_createPartitionKey(record), recordBytes);
    if (isUpdated) {
      await _cache.set(record.key, recordBytes);
    }
    return isUpdated;
  }

  Future<bool> delete(String key) async {
    await _cache.invalidate(key);
    return await _storage.delete(key);
  }

  Stream<RecordEvent> watch() {
    return _storage.watch();
  }

  invalidateCache() {
    _cache = MapCache.lru(maximumSize: _cacheSize);
  }

  Future<bool> destroy() async {
    if (await _storageDirectory.exists()) {
      await _storageDirectory.delete(recursive: true);
      return true;
    }
    return false;
  }
}

class RecordStore {
  final Directory directory;
  static final RegExp filePattern = RegExp(r'^[A–Za–z0–9._-]*');

  RecordStore({required this.directory});

  bool _checkFileNamePattern(String fileName) {
    if (fileName.length > 255 ||
        !fileName.split("/").every(filePattern.hasMatch)) {
      throw FormatException(
          "Record Key field should not be blank or greater than 255 in length and allows alphanumeric characters before and after path separator");
    }
    return true;
  }

  String _generateKey(String filePath) {
    return filePath.split("/").reversed.take(2).toList().reversed.join("/");
  }

  Future<void> initialize() async {
    if (!await directory.exists()) {
      await directory.create();
      print("Directory ${directory.path} created.");
    }
  }

  Stream<String> get keys {
    return directory
        .list(recursive: true)
        .where(((FileSystemEntity file) => file is File))
        .map((file) => _generateKey(file.path))
        .where(_checkFileNamePattern);
  }

  Future<List<int>?> find(String key) async {
    _checkFileNamePattern(key);
    File recordFile = File("${directory.path}/$key");
    return await recordFile.exists()
        ? recordFile.readAsBytes()
        : Future.value(null);
  }

  Future<bool> insert(String key, List<int> bytes) async {
    _checkFileNamePattern(key);
    File recordFile = File("${directory.path}/$key");

    if (await recordFile.exists()) {
      return false;
    }
    await (await recordFile.create(recursive: true)).writeAsBytes(bytes);
    return true;
  }

  Future<bool> update(String key, List<int> bytes) async {
    _checkFileNamePattern(key);
    File recordFile = File("${directory.path}/$key");
    return await recordFile.exists()
        ? recordFile.writeAsBytes(bytes).then((value) => true)
        : Future.value(false);
  }

  Future<bool> delete(String key) async {
    _checkFileNamePattern(key);
    File recordFile = File("${directory.path}/$key");
    return await recordFile.exists()
        ? recordFile.delete().then((value) => true)
        : Future.value(false);
  }

  Stream<RecordEvent> watch() {
    return directory
        .watch(events: FileSystemEvent.all)
        .map((FileSystemEvent event) {
      String keyValue = _generateKey(event.path);
      switch (event.type) {
        case FileSystemEvent.create:
          return RecordEvent(key: keyValue, type: RecordEventType.create);
        case FileSystemEvent.modify:
          return RecordEvent(key: keyValue, type: RecordEventType.modify);
        default:
          return RecordEvent(key: keyValue, type: RecordEventType.delete);
      }
    });
  }

  Future<void> destroy() async {
    if (await directory.exists()) {
      await directory.delete(recursive: true);
      print("Directory ${directory.path} deleted.");
    }
  }
}
