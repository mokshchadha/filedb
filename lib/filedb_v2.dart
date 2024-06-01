import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:basics/basics.dart';
import 'package:quiver/cache.dart';

enum RecordEventType { create, modify, delete }

final class RecordEvent {
  final String key;
  final RecordEventType type;

  RecordEvent(this.key, this.type);
}

abstract class Record<T> {
  static final String fieldSeparator = '|';
  final String key;

  Record(this.key);

  set key(String newKey) {
    this.key = newKey;
  }
}

abstract interface class RecordStore<T> {
  Future<void> init();
  Stream<String> get keys; // keep on getting the keys of collection
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
  late final int _maxRecordBytes; // system limit is 2kb

  //TODO: why are reader and writer separate and can there be multiple readers?
  late final RandomAccessFile
      _dataFileWriter; // rather than sequentially access, access from anywhere
  late final RandomAccessFile _dataFileReader;

  final Queue<int> deletedIndexes =
      Queue(); //NOTE: if an index is deleted in future rather than leaving that key empty we can reuse it for inserts later
  Completer<bool> _currentWriteOps = Completer();

  final Map<String, int> _metaMap =
      SplayTreeMap(); //map the key of the record to idx of file line

  int _metaIndex = 0; // idx of meta file to store new index entry in new line
  bool _metaChanged = false;

  MetaRecordStore(
      {required Directory directory,
      required maxRecordBytes,
      int flushIntervalMs = 250}) {
    _directory = directory;
    _maxRecordBytes = maxRecordBytes;
    _flushIntervalMS = flushIntervalMs;
    _currentWriteOps.complete(true);
  }

  _newMetaIndex() {
    return _metaIndex++; //TODO: can we remove _metaIndex??
  }

  Future<bool> _write(final Future<bool> Function() operation) {
    //what ever is the ops, write or update we control the completer here
    //NOTE: make sure to pass true (bool) on success as that is assigned to completer state
    Completer<bool> completer = Completer();
    if (_currentWriteOps.isCompleted) {
      operation().then(completer.complete);
    } else {
      _currentWriteOps.future
          .then((value) => operation().then(completer.complete));
    }
    // we are resolving the completer and appending to current ops
    _currentWriteOps = completer;
    return completer // even though _currentWrite Ops has same value can we just use that TODO:
        .future; // in order to chain the _write (masked under insert or update) operations
  }

  @override
  Future<void> init() async {
    if (!await _directory.exists()) {
      await _directory.create();
      final path = _directory.path;
      print('Dir $path created');

      final metaFilePath = '$path/meta.db';
      await File.fromUri(Uri.file(metaFilePath)).create();
      print('Meta file in $metaFilePath created');

      final dataFilePath = '$path/data.db';
      await File.fromUri(Uri.file(dataFilePath)).create();
      print('data file in $dataFilePath created');

      _dataFileReader =
          await File.fromUri(Uri.file(dataFilePath)).open(mode: FileMode.read);
      print('file opened in read mode');

      _dataFileWriter = await File.fromUri(Uri.file(dataFilePath)).open(
          mode: FileMode.writeOnly); // TODO: why write why not write only?
      print('file opened in write mode');

      Timer.periodic(Duration(milliseconds: _flushIntervalMS), (timer) async {
        _write(() async {
          if (_metaChanged) {
            print('flushing data file');
            _dataFileWriter.flush();

            print('updating meta file');
            await File.fromUri(Uri.file(metaFilePath))
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
    Map<String, dynamic> json = metaFileContents.trim().isNotEmpty
        ? jsonDecode(metaFileContents)
        : jsonDecode('{}');
    for (MapEntry<String, dynamic> entry in json.entries) {
      _metaMap[entry.key] = entry.value as int; // caching the meta file
    }
  }

  @override
  Stream<String> get keys => Stream.fromIterable(_metaMap.keys);

  @override
  Future<List<int>?> find(String key) async {
    int? index = _metaMap.get(key);
    if (index == null) return null;

    await _dataFileReader
        .setPosition(index * _maxRecordBytes); // set the pointer to start
    List<int> buffer =
        Int8List(_maxRecordBytes); //max possible size of the record
    await _dataFileReader.readInto(buffer, 0); // put data into the buffer
    return buffer;
  }

  @override
  Future<bool> insert(String key, List<int> bytes) {
    return _write(() async {
      int? index = _metaMap[key];
      if (index != null) {
        return false; // TODO: since the operation is false will the completer keep retrying?
      }
      index = _metaMap.putIfAbsent(key, () {
        if (deletedIndexes.isNotEmpty) {
          return deletedIndexes.removeFirst(); //TODO: why this?
        }
        return _newMetaIndex();
      });

      await _dataFileWriter.setPosition(index * _maxRecordBytes);
      await _dataFileWriter.writeFrom(bytes, 0);
      _metaChanged =
          true; // so the next time we flush to the file we update meta file as well
      return true;
    });
  }

  @override
  Future<bool> update(String key, List<int> bytes) {
    return _write(() async {
      int? index = _metaMap.get(key);
      if (index == null) {
        return false; //TODO: check if this works or goes in loop
      }
      await _dataFileWriter.setPosition(index * _maxRecordBytes);
      await _dataFileWriter.writeFrom(bytes, 0);
      _metaChanged = true; //TODO: Not sure if this be used?
      return true;
    });
  }

  @override
  Stream<RecordEvent> watch() {
    // TODO: implement watch
    throw UnimplementedError();
  }

  @override
  Future<bool> delete(String key) {
    //delete is also a write operation
    return _write(() async {
      int? index = _metaMap.get(key);
      if (index == null) {
        return false; //TODO: check for deletion
      }
      deletedIndexes.addLast(_metaMap.remove(key) as int);
      return true;
    });
  }

  @override
  Future<void> destroy() async {
    if (await _directory.exists()) {
      await _directory.delete(recursive: true);
      print("Dir ${_directory.path} deleted.");
    }
  }
}

final class FileStore implements RecordStore<List<int>> {
  final Directory _directory;
  static final RegExp filePattern = RegExp(r'^[A–Za–z0–9._-]*');

  FileStore({required Directory directory}) : _directory = directory;

  bool _checkFileNamePattern(String fileName) {
    //checking file name pattern to avoid weird files names like . # % @ etc
    if (fileName.length > 255 ||
        !fileName.split("/").every(filePattern.hasMatch)) {
      throw FormatException(
          "Record Key field should not be blank or greater than 255 in length and allows alphanumeric characters before and after path separator");
    }
    return true;
  }

  String _generateKey(String filePath) {
    return filePath.split('/').reversed.take(2).toList().reversed.join('/');
  }

  @override
  Future<void> init() async {
    if (!await _directory.exists()) {
      await _directory.create();
      print('dir created ${_directory.path} in FileStore');
    }
  }

  @override
  Stream<String> get keys => _directory
      .list(recursive: true)
      .where((FileSystemEntity file) => file is File)
      .map((file) => _generateKey(file.path))
      .where(_checkFileNamePattern);

  @override
  Future<List<int>?> find(String key) async {
    _checkFileNamePattern(key);
    File recordFile = File('${_directory.path}/$key');
    return await recordFile.exists()
        ? recordFile.readAsBytes()
        : Future.value(null);
  }

  @override
  Future<bool> insert(String key, List<int> bytes) async {
    _checkFileNamePattern(key);
    File recordFile = File('${_directory.path}/$key');
    if (await recordFile.exists()) {
      return false;
    }
    await (await recordFile.create(recursive: true))
        .writeAsBytes(bytes); //TODO: use .. operator here
    return true;
  }

  @override
  Future<bool> update(String key, List<int> bytes) async {
    _checkFileNamePattern(key);
    final recordFile = File('${_directory.path}/$key');
    return await recordFile.exists()
        ? recordFile.writeAsBytes(bytes).then((_) => true)
        : Future.value(false);
  }

  @override
  Future<bool> delete(String key) async {
    _checkFileNamePattern(key);
    File recordFile = File('${_directory.path}/$key');
    return await recordFile.exists()
        ? recordFile.delete().then((_) => true)
        : Future.value(false);
  }

  @override
  Future<void> destroy() async {
    if (await _directory.exists()) {
      await _directory.delete(recursive: true);
      print('Dir ${_directory.path} is destroyed');
    }
  }

  @override
  Stream<RecordEvent> watch() {
    return _directory
        .watch(events: FileSystemEvent.all)
        .map((FileSystemEvent event) {
      String keyValue = _generateKey(event.path);
      switch (event.type) {
        case FileSystemEvent.create:
          return RecordEvent(keyValue, RecordEventType.create);
        case FileSystemEvent.modify:
          return RecordEvent(keyValue, RecordEventType.modify);
        case FileSystemEvent.delete:
          return RecordEvent(keyValue, RecordEventType.delete);
        default:
          return RecordEvent(keyValue,
              RecordEventType.delete); //TODO: Not sure if default be delete
      }
    });
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
    _cache =
        MapCache.lru(maximumSize: _cacheSize); // lru cache min 100 to max 1M
  }

  String serialize(T record);

  T deserialize(String data);

  bool validate(T record);

  String _createPartitionKey(T record) {
    String pKey = partitionKey(record);
    if (pKey.isBlank || pKey.length > 255) {
      throw FormatException(
          'Partition key label cannot be blank or greater than 255 char $pKey');
    }
    return '$pKey/${record.key}';
  }

  Future<void> init() async {
    await _storage.destroy();
    await _storage.init();
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
            'Record validation failed while fetching from disk.');
      }
      return record;
    }
    return null;
  }

  Future<bool> insert(T record) async {
    if (!validate(record)) {
      throw FormatException('Record validation failed while insertion');
    }
    return _storage.insert(
        _createPartitionKey(record), utf8.encode(serialize(record)));
  }

  Future<bool> update(T record) async {
    if (!validate(record)) {
      throw FormatException('Record validation failed while update');
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

  Future<void> destroy(async) {
    return _storage.destroy();
  }
}

// - usage
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
  await collection.init();
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
