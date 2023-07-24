import 'basics.dart';

class ChatappEvent extends Record<ChatappEvent> {
  final int id;
  final String sys_msg_id;
  final String message_id;
  final String sender_name;
  final String event_direction;
  final String from;
  final String to;
  final String event_type;
  final String contextual_message_id;
  final String sender;

  final String received_at;

  ChatappEvent(super.key,
      {required this.id,
      required this.sys_msg_id,
      required this.message_id,
      required this.sender,
      required this.event_direction,
      required this.from,
      required this.to,
      required this.event_type,
      required this.contextual_message_id,
      required this.sender_name,
      required this.received_at});
}

// user
class ChatappEventCollection extends RecordCollection<ChatappEvent> {
  ChatappEventCollection(
      {required super.storageDirectory,
      super.cacheSize,
      required super.partitionKey});

  @override
  ChatappEvent deserialize(String data) {
    List<String> field = data.split(Record.fieldSeparator);
    return ChatappEvent(
        id: int.parse(field[0]),
        sys_msg_id: field[1],
        message_id: field[2],
        from: (field[3]),
        to: field[4],
        sender_name: field[5],
        event_direction: (field[6]),
        received_at: field[7],
        event_type: field[8],
        contextual_message_id: field[9],
        sender: field[10],
        field[11]);
  }

  @override
  String serialize(ChatappEvent record) {
    var buffer = StringBuffer()
      ..writeAll([
        record.key,
        record.id,
        record.sys_msg_id,
        record.message_id.toString(),
        record.from,
        record.to,
        record.sender_name.toString(),
        record.event_direction,
        record.received_at,
        record.event_type,
        record.contextual_message_id,
        record.sender
      ], Record.fieldSeparator);
    return buffer.toString();
  }

  @override
  bool validate(ChatappEvent record) {
    // if (record.reference.isBlank) {
    //   return false;
    // }
    //TODO: other fields validations
    return true;
  }
}
