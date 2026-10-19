// How a dead letter's position and its replay target read for each kind of sink and source:
// Kafka has partitions, offsets and topics; NATS JetStream has stream sequences and subjects.

export type LetterPosition = {
  offset: number;
  partition: number;
};

export function positionText(letter: LetterPosition, kind?: string): string {
  return kind === 'nats'
    ? `sequence ${letter.offset}`
    : `partition ${letter.partition} · offset ${letter.offset}`;
}

export function targetNoun(kind?: string): string {
  return kind === 'nats' ? 'subject' : 'topic';
}
