// ignore_for_file: prefer_function_declarations_over_variables

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:basics/basics.dart';
import 'package:quiver/cache.dart';

// https://docs.kernel.org/filesystems/ext4/overview.html
// large_dir, dir_index, inline_data, metadata_csum, delalloc, min_batch_time=100,000us, max_batch_time=500,000us
// MGLRU Page cache, flush interval, block size, block group size, journal WAL and encryption management is upto the underlying FS and OS.

enum RecordEventType { create, modify, delete }

final class RecordEvent {
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

abstract interface class RecordStore<T> {
  Future<void> initialize();
  Stream<String> get keys;
  Future<T?> find(String key);
  Future<bool> insert(String key, T bytes);
  Future<bool> update(String key, T bytes);
  Future<bool> delete(String key);
  Stream<RecordEvent> watch();
  Future<void> destroy();
}

final class MetaRecordStore implements RecordStore<List<int>> {
  late final Directory _directory;
  late final int _flushIntervalMS;
  late final int _maxRecordBytes;

  late final RandomAccessFile _dataFileWriter;
  late final RandomAccessFile _dataFileReader;

  final Queue<int> deletedIndexes = Queue();
  Completer<bool> _currentWriteOps = Completer();

  final Map<String, int> _metaMap = SplayTreeMap();
  int _metaIndex = 0;
  bool _metaChanged = false;

  MetaRecordStore(
      {required Directory directory,
      required maxRecordBytes,
      int flushIntervalMS = 250}) {
    _directory = directory;
    _maxRecordBytes = maxRecordBytes;
    _flushIntervalMS = flushIntervalMS;
    _currentWriteOps.complete(true);
  }

  Future<bool> _write(final Future<bool> Function() operation) {
    Completer<bool> completer = Completer();
    if (_currentWriteOps.isCompleted) {
      operation().then(completer.complete);
    } else {
      _currentWriteOps.future
          .then((value) => operation().then(completer.complete));
    }
    _currentWriteOps = completer;
    return completer.future;
  }

  _newMetaIndex() {
    return _metaIndex++;
  }

  @override
  Future<void> initialize() async {
    if (!await _directory.exists()) {
      await _directory.create();
      print("Directory ${_directory.path} created.");

      await File.fromUri(Uri.file('${_directory.path}/meta.db')).create();
      print("Meta file in Directory ${_directory.path} created.");

      String dataFilePath = '${_directory.path}/data.db';
      await File.fromUri(Uri.file(dataFilePath)).create();
      print("Data file in Directory ${_directory.path} created.");

      print("Open data file in write mode");
      _dataFileWriter =
          await File.fromUri(Uri.file(dataFilePath)).open(mode: FileMode.write);

      print("Open data file in read mode");
      _dataFileReader =
          await File.fromUri(Uri.file(dataFilePath)).open(mode: FileMode.read);

      Timer.periodic(Duration(milliseconds: _flushIntervalMS), (timer) async {
        _write(() async {
          if (_metaChanged) {
            print("flushing data file");
            _dataFileWriter.flush();

            print("write meta file");
            await File.fromUri(Uri.file('${_directory.path}/meta.db'))
                .writeAsString(jsonEncode(_metaMap));
            _metaChanged = false;
          }
          return true;
        });
      });
    }

    String metaFileContents =
        await File.fromUri(Uri.file('${_directory.path}/meta.db'))
            .readAsString();
    Map<String, dynamic> json = jsonDecode(
        metaFileContents.trim().isNotEmpty ? metaFileContents : "{}");
    for (MapEntry<String, dynamic> entry in json.entries) {
      _metaMap[entry.key] = entry.value as int;
    }
  }

  @override
  Stream<String> get keys => Stream.fromIterable(_metaMap.keys);

  @override
  Future<List<int>?> find(String key) async {
    int? index = _metaMap.get(key);
    if (index == null) {
      return null;
    }
    await _dataFileReader.setPosition(index * _maxRecordBytes);
    List<int> buffer = Int8List(_maxRecordBytes);
    await _dataFileReader.readInto(buffer, 0);
    return buffer;
  }

  @override
  Future<bool> insert(String key, List<int> bytes) async {
    return _write(() async {
      int? index = _metaMap.get(key);
      if (index != null) {
        return false;
      }

      index = _metaMap.putIfAbsent(
          key,
          () => deletedIndexes.isNotEmpty
              ? deletedIndexes.removeFirst()
              : _newMetaIndex());
      await _dataFileWriter.setPosition(index * _maxRecordBytes);
      await _dataFileWriter.writeFrom(bytes, 0);
      _metaChanged = true;
      return true;
    });
  }

  @override
  Future<bool> update(String key, List<int> bytes) async {
    return _write(() async {
      int? index = _metaMap.get(key);
      if (index == null) {
        return false;
      }
      await _dataFileWriter.setPosition(index * _maxRecordBytes);
      await _dataFileWriter.writeFrom(bytes, 0);
      _metaChanged = true;
      return true;
    });
  }

  @override
  Stream<RecordEvent> watch() {
    // TODO: implement watch
    throw UnimplementedError();
  }

  @override
  Future<bool> delete(String key) async {
    return _write(() async {
      int? index = _metaMap.get(key);
      if (index == null) {
        return false;
      }
      deletedIndexes.addLast(_metaMap.remove(key) as int);
      return true;
    });
  }

  @override
  Future<void> destroy() async {
    if (await _directory.exists()) {
      await _directory.delete(recursive: true);
      print("Directory ${_directory.path} deleted.");
    }
  }
}

final class FileStore implements RecordStore<List<int>> {
  final Directory _directory;
  static final RegExp filePattern = RegExp(r'^[A–Za–z0–9._-]*');

  FileStore({required Directory directory}) : _directory = directory;

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

  @override
  Future<void> initialize() async {
    if (!await _directory.exists()) {
      await _directory.create();
      print("Directory ${_directory.path} created.");
    }
  }

  @override
  Stream<String> get keys {
    return _directory
        .list(recursive: true)
        .where(((FileSystemEntity file) => file is File))
        .map((file) => _generateKey(file.path))
        .where(_checkFileNamePattern);
  }

  @override
  Future<List<int>?> find(String key) async {
    _checkFileNamePattern(key);
    File recordFile = File("${_directory.path}/$key");
    return await recordFile.exists()
        ? recordFile.readAsBytes()
        : Future.value(null);
  }

  @override
  Future<bool> insert(String key, List<int> bytes) async {
    _checkFileNamePattern(key);
    File recordFile = File("${_directory.path}/$key");

    if (await recordFile.exists()) {
      return false;
    }
    await (await recordFile.create(recursive: true)).writeAsBytes(bytes);
    return true;
  }

  @override
  Future<bool> update(String key, List<int> bytes) async {
    _checkFileNamePattern(key);
    File recordFile = File("${_directory.path}/$key");
    return await recordFile.exists()
        ? recordFile.writeAsBytes(bytes).then((value) => true)
        : Future.value(false);
  }

  @override
  Future<bool> delete(String key) async {
    _checkFileNamePattern(key);
    File recordFile = File("${_directory.path}/$key");
    return await recordFile.exists()
        ? recordFile.delete().then((value) => true)
        : Future.value(false);
  }

  @override
  Stream<RecordEvent> watch() {
    return _directory
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

  @override
  Future<void> destroy() async {
    if (await _directory.exists()) {
      await _directory.delete(recursive: true);
      print("Directory ${_directory.path} deleted.");
    }
  }
}

abstract class RecordCollection<T extends Record> {
  late final RecordStore _storage;
  late final int _cacheSize;
  late MapCache<String, List<int>> _cache;
  late String Function(T) partitionKey;

  RecordCollection(
      {required RecordStore storage,
      required this.partitionKey,
      cacheSize = 100}) {
    _storage = storage;
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

  Future<void> initialize() async {
    await _storage.destroy();
    await _storage.initialize();
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

    return null;
  }

  Future<bool> insert(T record) async {
    if (!validate(record)) {
      throw FormatException("Record validation failed while insertion.");
    }
    return _storage.insert(
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

  Future<void> destroy() async {
    return _storage.destroy();
  }
}

// user will extends Record and will make sure avg total bytes are at least 2KB
class TradeEntry extends Record<TradeEntry> {
  final String reference;
  final String period;
  final double dataValue;
  final String status;
  final String units;
  final int magnitude;
  final String subject;
  final String group;
  final String seriesTitle1;
  final String seriesTitle2;
  final String seriesTitle3;

  TradeEntry(
      super.key,
      this.reference,
      this.period,
      this.dataValue,
      this.status,
      this.units,
      this.magnitude,
      this.subject,
      this.group,
      this.seriesTitle1,
      this.seriesTitle2,
      this.seriesTitle3);
}

// user
class TradeEntryCollection extends RecordCollection<TradeEntry> {
  TradeEntryCollection(
      {required super.storage, super.cacheSize, required super.partitionKey});

  @override
  TradeEntry deserialize(String data) {
    List<String> field = data.split(Record.fieldSeparator);
    return TradeEntry(
        field[0],
        field[1],
        field[2],
        double.parse(field[3]),
        field[4],
        field[5],
        int.parse(field[6]),
        field[7],
        field[8],
        field[9],
        field[10],
        field[11]);
  }

  @override
  String serialize(TradeEntry record) {
    var buffer = StringBuffer()
      ..writeAll([
        record.key,
        record.reference,
        record.period,
        record.dataValue.toString(),
        record.status,
        record.units,
        record.magnitude.toString(),
        record.subject,
        record.group,
        record.seriesTitle1,
        record.seriesTitle2,
        record.seriesTitle3
      ], Record.fieldSeparator);
    return buffer.toString();
  }

  @override
  bool validate(TradeEntry record) {
    if (record.reference.isBlank) {
      return false;
    }
    //TODO: other fields validations
    return true;
  }
}

void main(List<String> args) async {
  var collection = TradeEntryCollection(
    storage:
        MetaRecordStore(directory: Directory(args[1]), maxRecordBytes: 512),
    cacheSize: 100000,
    partitionKey: (TradeEntry record) => record.status,
  );
  await collection.initialize();
  print("collection directory created");

  sleep(Duration(seconds: 3));
  int count = 100000;
  List<String> lines = await File(args.first).readAsLines();
  var stopwatch = Stopwatch()..start();
  for (String line in lines) {
    if (count == 100000) {
      count++;
      continue;
    }

    List<String> record = line.split(',');

    await collection.insert(TradeEntry(
        (count++).toString(),
        record[0],
        record[1],
        record[2].isNotEmpty ? double.parse(record[2]) : 0.0,
        record[3],
        record[4],
        int.parse(record[5]),
        record[6],
        record[7],
        record[8],
        record[9],
        record[10]));
  }
  print('writing records executed in ${stopwatch.elapsed}');

  sleep(Duration(seconds: 3));
  List<String> keys = await collection.keys.toList();

  stopwatch = Stopwatch()..start();
  for (String key in keys) {
    TradeEntry? entry = await collection.find(key);
    if ((entry != null ? entry.dataValue : 0.0) > 750000000.0) {
      break;
    }
  }
  print('fetch record without cache records executed in ${stopwatch.elapsed}');

  sleep(Duration(seconds: 3));
  stopwatch = Stopwatch()..start();
  for (String key in keys) {
    TradeEntry? entry = await collection.find(key);
    if ((entry != null ? entry.dataValue : 0.0) > 750000000.0) {
      break;
    }
  }
  print(
      'fetch record with in memory cache records executed in ${stopwatch.elapsed}');
}
