import styled from 'styled-components';

import BlockType from '@interfaces/BlockType';
import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { PythonWorkerType } from './PythonWorker';
import { UNIT } from '@oracle/styles/units/spacing';

const TableStyle = styled.table`
  border-collapse: collapse;
  margin-top: ${UNIT}px;
  width: 100%;

  td, th {
    padding: ${UNIT * 0.5}px ${UNIT}px ${UNIT * 0.5}px 0;
    text-align: left;
    vertical-align: top;
    ${props => `border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }

  td:first-child {
    max-width: ${UNIT * 18}px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
`;

// Where each Python block of the saved pipeline runs: its own worker, the pipeline's
// default, or MageML's Python, and whether that worker is ready.
function BlockWorkers({ blocks, defaultWorker, workers }: {
  blocks: BlockType[];
  defaultWorker?: string;
  workers: PythonWorkerType[];
}) {
  const python = blocks.filter(block => block?.language === 'python');
  if (!python.length) {
    return null;
  }

  return (
    <TableStyle>
      <thead>
        <tr>
          {['Block', 'Runs in', ''].map(title => (
            <th key={title}><Text bold muted small>{title}</Text></th>
          ))}
        </tr>
      </thead>
      <tbody>
        {python.map((block) => {
          const own = block?.configuration?.python_worker;
          const chosen = own || defaultWorker;
          const builtin = !chosen || chosen === 'mage';
          const worker = builtin ? null : workers.find(w => w.name === chosen);
          let state = null;
          if (!builtin && !worker) {
            state = <Text danger small>Missing</Text>;
          } else if (worker && !worker.prepared) {
            state = <Text small warning>Not prepared</Text>;
          } else {
            state = <Text muted small>Ready</Text>;
          }

          return (
            <tr key={block.uuid}>
              <td title={block.uuid}><Text monospace small>{block.uuid}</Text></td>
              <td title={builtin ? undefined : chosen}>
                <Text small>
                  {builtin ? 'MageML\'s Python' : (worker?.label || chosen)}
                </Text>
                {!own && !builtin && <Text muted xsmall>the pipeline&apos;s default</Text>}
              </td>
              <td>{state}</td>
            </tr>
          );
        })}
      </tbody>
    </TableStyle>
  );
}

export default BlockWorkers;
