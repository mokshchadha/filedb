// ignore_for_file: prefer_function_declarations_over_variables

import 'dart:io';

import 'collections/ChatappEvents.dart';

// https://docs.kernel.org/filesystems/ext4/overview.html
// large_dir, dir_index, inline_data, metadata_csum, delalloc, min_batch_time=100,000us, max_batch_time=500,000us
// MGLRU Page cache, flush interval, block size, block group size, journal WAL and encryption management is upto the underlying FS and OS.
// user will extends Record and will make sure avg total bytes are at least 2KB

void main(List<String> args) async {
  var collection = ChatappEventCollection(
      storageDirectory: Directory(args[1]),
      cacheSize: 100000,
      partitionKey: (ChatappEvent record) => record.event_direction);
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

    await collection.insert(ChatappEvent(
      record[0],
      id: int.parse(record[0]),
      sys_msg_id: record[1],
      message_id: record[2],
      from: record[3],
      to: record[4],
      sender_name: record[5],
      event_direction: record[6],
      received_at: record[7],
      event_type: record[8],
      contextual_message_id: record[9],
      sender: record[10],
    ));
  }
  print('writing records executed in ${stopwatch.elapsed}');

  List<String> keys = await collection.keys.toList();

  sleep(Duration(seconds: 3));
  stopwatch = Stopwatch()..start();
  for (String key in keys) {
    ChatappEvent? entry = await collection.find(key);
    if ((entry != null ? entry.id : 0.0) > 750000000.0) {
      break;
    }
  }
  print('fetch record without cache records executed in ${stopwatch.elapsed}');

  sleep(Duration(seconds: 3));
  stopwatch = Stopwatch()..start();
  for (String key in keys) {
    ChatappEvent? entry = await collection.find(key);
    if ((entry != null ? entry.id : 0.0) > 750000000.0) {
      break;
    }
  }
  print(
      'fetch record with in memory cache records executed in ${stopwatch.elapsed}');
}
