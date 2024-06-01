import 'dart:io';

import 'package:filedb/filedb.dart';
import 'package:test/test.dart';

void main(List<String> args) async {
  print(args);
  var collection = TradeEntryCollection(
      storage:
          MetaRecordStore(directory: Directory(args[1]), maxRecordBytes: 512),
      cacheSize: 100000,
      partitionKey: (TradeEntry record) => record.status);
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
