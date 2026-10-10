import { FormEvent, useEffect, useId, useRef, useState } from 'react';

import { OPERATOR_LABELS, filterError, needsValue, operatorsFor, placeholderFor } from './logic';
import { AtlasColumn, AtlasFilter, FilterOperator } from './types';

export function FilterEditor({
  columns,
  initialColumn,
  onAdd,
  onCancel,
}: {
  columns: AtlasColumn[];
  initialColumn?: number | null;
  onAdd: (filter: AtlasFilter) => void;
  onCancel: () => void;
}) {
  const id = useId();
  const [columnId, setColumnId] = useState<number>(initialColumn ?? columns[0]?.id ?? 0);
  const column = columns.find(item => item.id === columnId) ?? columns[0];
  const operators = column ? operatorsFor(column) : [];
  const [op, setOp] = useState<FilterOperator>(operators[0] ?? 'is_null');
  const [value, setValue] = useState('');
  const [high, setHigh] = useState('');
  const [touched, setTouched] = useState(false);
  const valueRef = useRef<HTMLInputElement>(null);
  const operator = operators.includes(op) ? op : operators[0] ?? 'is_null';
  const filter: AtlasFilter = {
    column_id: columnId,
    op: operator,
    ...(needsValue(operator) ? { value } : {}),
    ...(operator === 'between' ? { high } : {}),
  };
  const error = column ? filterError(filter, column) : 'Choose a column';

  useEffect(() => {
    valueRef.current?.focus();
  }, [columnId, operator]);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    setTouched(true);
    if (!error) onAdd(filter);
  };

  return (
    <form
      className="ca-filter-editor"
      onKeyDown={event => {
        if (event.key === 'Escape') {
          event.stopPropagation();
          onCancel();
        }
      }}
      onSubmit={submit}
    >
      <label htmlFor={`${id}-column`}>Column</label>
      <select
        id={`${id}-column`}
        onChange={event => setColumnId(Number(event.target.value))}
        value={columnId}
      >
        {columns.map(item => (
          <option key={item.id} value={item.id}>
            {item.name}
          </option>
        ))}
      </select>
      <label htmlFor={`${id}-op`}>Condition</label>
      <select id={`${id}-op`} onChange={event => setOp(event.target.value as FilterOperator)} value={operator}>
        {operators.map(item => (
          <option key={item} value={item}>
            {OPERATOR_LABELS[item]}
          </option>
        ))}
      </select>
      {needsValue(operator) && column && (
        <>
          <label htmlFor={`${id}-value`}>{operator === 'between' ? 'From' : 'Value'}</label>
          <input
            id={`${id}-value`}
            inputMode={column.kind === 'numeric' ? 'decimal' : undefined}
            onChange={event => setValue(event.target.value)}
            placeholder={placeholderFor(column)}
            ref={valueRef}
            value={value}
          />
        </>
      )}
      {operator === 'between' && column && (
        <>
          <label htmlFor={`${id}-high`}>To</label>
          <input
            id={`${id}-high`}
            inputMode={column.kind === 'numeric' ? 'decimal' : undefined}
            onChange={event => setHigh(event.target.value)}
            placeholder={placeholderFor(column)}
            value={high}
          />
        </>
      )}
      <div className="ca-filter-actions">
        {touched && error && <span className="ca-error-text">{error}</span>}
        <button className="ca-button" onClick={onCancel} type="button">
          Cancel
        </button>
        <button className="ca-button ca-button-primary" type="submit">
          Add filter
        </button>
      </div>
    </form>
  );
}
