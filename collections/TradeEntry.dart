import 'package:basics/basics.dart';

import 'basics.dart';

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
      {required super.storageDirectory,
      super.cacheSize,
      required super.partitionKey});

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
