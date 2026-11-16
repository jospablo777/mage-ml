import styled from 'styled-components';
import { useMemo } from 'react';

import FlexContainer from '@oracle/components/FlexContainer';
import LineSeries from '@components/charts/LineSeries';
import PrivateRoute from '@components/shared/PrivateRoute';
import SettingsDashboard from '@components/settings/Dashboard';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { PADDING_UNITS, UNIT } from '@oracle/styles/units/spacing';
import { SECTION_ITEM_UUID_SERVER_HEALTH, SECTION_UUID_WORKSPACE } from
  '@components/settings/Dashboard/constants';
import { dateFormatLong } from '@utils/date';
import { numberWithCommas } from '@utils/string';

type HealthType = {
  history: {
    load_1: number;
    mageml_memory_mb: number;
    memory_used_mb: number;
    processes: number;
    time: string;
  }[];
  machine: {
    cpu_count: number;
    load_1: number;
    load_15: number;
    load_5: number;
    memory_total_mb?: number;
    memory_used_mb?: number;
  };
  processes: {
    cpu_percent: number;
    kind: string;
    memory_mb: number;
    name: string;
    pid: number;
    started_at: string;
  }[];
  sample_seconds: number;
  storage?: {
    [name: string]: { bytes?: number; free_bytes?: number; path?: string; total_bytes?: number };
  } & { measured_at?: string };
};

const TileStyle = styled.div`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  margin: 0 ${UNIT}px ${UNIT}px 0;
  min-width: ${UNIT * 24}px;
  padding: ${UNIT * 1.5}px ${UNIT * 2}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const TableStyle = styled.table`
  border-collapse: collapse;
  width: 100%;

  td, th {
    padding: ${UNIT * 0.75}px ${UNIT}px ${UNIT * 0.75}px 0;
    text-align: left;
    ${props => `border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }
`;

function bytes(value?: number): string {
  if (value === null || value === undefined) {
    return '-';
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }

  return `${size.toFixed(unit ? 1 : 0)} ${units[unit]}`;
}

function megabytes(value?: number): string {
  return value === null || value === undefined ? '-' : bytes(value * 1024 * 1024);
}

function Tile({ label, note, value }: { label: string; note?: string; value: string }) {
  return (
    <TileStyle>
      <Text muted small>{label}</Text>
      <Text bold large>{value}</Text>
      {note && <Text muted xsmall>{note}</Text>}
    </TileStyle>
  );
}

function Chart({ data, label, unit }: {
  data: { x: number; y: number[] }[];
  label: string;
  unit: string;
}) {
  return (
    <Spacing mb={3}>
      <Text bold>{label}</Text>
      {data.length < 2 ? (
        <Text muted small>Sampled every 30 seconds; the line appears after the second sample.</Text>
      ) : (
        <LineSeries
          data={data}
          getX={point => point.x}
          gridProps={{ strokeDasharray: null, strokeOpacity: 0.15 }}
          height={180}
          hideGridX
          margin={{ bottom: 30, left: 45, right: 10, top: 10 }}
          noCurve
          numYTicks={4}
          renderXTooltipContent={(point: any) => (
            <Text small>{dateFormatLong(new Date(point.x).toISOString())}</Text>
          )}
          renderYTooltipContent={(point: any) => (
            <Text small>{`${point?.y?.[0] ?? point?.y} ${unit}`}</Text>
          )}
          xLabelFormat={x => new Date(x).toISOString().slice(11, 16)}
          xLabelRotate={false}
        />
      )}
    </Spacing>
  );
}

function ServerHealthPage() {
  const { data } = api.server_health.detail('current', {}, { refreshInterval: 30000 });
  const health: HealthType = data?.server_health;

  const charts = useMemo(() => {
    const history = health?.history || [];
    return {
      load: history.map(point => ({ x: Date.parse(point.time), y: [point.load_1] })),
      memory: history.map(point => ({ x: Date.parse(point.time), y: [point.mageml_memory_mb] })),
    };
  }, [health]);

  if (!health) {
    return (
      <SettingsDashboard
        title="Server health"
        uuidItemSelected={SECTION_ITEM_UUID_SERVER_HEALTH}
        uuidWorkspaceSelected={SECTION_UUID_WORKSPACE}
      >
        <Spacing p={PADDING_UNITS}><Spinner inverted /></Spacing>
      </SettingsDashboard>
    );
  }

  const { machine, processes, storage } = health;
  const mageMemory = processes.reduce((sum, p) => sum + (p.memory_mb || 0), 0);
  const disk = storage?.disk;

  return (
    <SettingsDashboard
      title="Server health"
      uuidItemSelected={SECTION_ITEM_UUID_SERVER_HEALTH}
      uuidWorkspaceSelected={SECTION_UUID_WORKSPACE}
    >
      <Spacing p={PADDING_UNITS}>
        <Text bold large>
          MageML uses {megabytes(mageMemory)} in {processes.length} process
          {processes.length === 1 ? '' : 'es'}; the load is {machine.load_1} on{' '}
          {machine.cpu_count} CPUs
        </Text>

        <Spacing mt={2}>
          <FlexContainer flexWrap="wrap">
            <Tile
              label="Load, last minute"
              note={`5 min ${machine.load_5}, 15 min ${machine.load_15}`}
              value={`${machine.load_1} / ${machine.cpu_count} CPUs`}
            />
            <Tile
              label="Memory used"
              note={machine.memory_total_mb ? `of ${megabytes(machine.memory_total_mb)}` : null}
              value={megabytes(machine.memory_used_mb)}
            />
            <Tile label="MageML processes" value={megabytes(mageMemory)} />
            {disk && (
              <Tile
                label="Disk free"
                note={`of ${bytes(disk.total_bytes)}`}
                value={bytes(disk.free_bytes)}
              />
            )}
          </FlexContainer>
        </Spacing>

        <Spacing mt={2}>
          <Chart data={charts.load} label="Load, last minute (last 6 hours)" unit="" />
          <Chart data={charts.memory} label="Memory of MageML's processes (last 6 hours)" unit="MB" />
        </Spacing>

        <Spacing mt={2}>
          <Text bold>Processes</Text>
          <TableStyle>
            <thead>
              <tr>
                {['Kind', 'PID', 'Name', 'CPU', 'Memory', 'Started'].map(label => (
                  <th key={label}><Text bold muted small>{label}</Text></th>
                ))}
              </tr>
            </thead>
            <tbody>
              {processes.map(process => (
                <tr key={process.pid}>
                  <td><Text small>{process.kind}</Text></td>
                  <td><Text monospace small>{process.pid}</Text></td>
                  <td><Text monospace muted small>{process.name}</Text></td>
                  <td><Text monospace small>{process.cpu_percent}%</Text></td>
                  <td><Text monospace small>{megabytes(process.memory_mb)}</Text></td>
                  <td><Text monospace muted small>{dateFormatLong(process.started_at)}</Text></td>
                </tr>
              ))}
            </tbody>
          </TableStyle>
        </Spacing>

        <Spacing mt={3}>
          <Text bold>Storage</Text>
          {!storage ? (
            <Text muted small>Measured in the background a few seconds after the server starts.</Text>
          ) : (
            <TableStyle>
              <tbody>
                {['variables', 'database'].filter(name => storage[name]).map(name => (
                  <tr key={name}>
                    <td>
                      <Text small>
                        {name === 'variables' ? 'Outputs and logs' : 'Metadata database'}
                      </Text>
                    </td>
                    <td><Text monospace muted small>{storage[name].path || '-'}</Text></td>
                    <td><Text monospace small>{bytes(storage[name].bytes)}</Text></td>
                  </tr>
                ))}
              </tbody>
            </TableStyle>
          )}
          {storage?.measured_at && (
            <Text muted xsmall>
              Measured {dateFormatLong(storage.measured_at)}, every 10 minutes
              {' '}({numberWithCommas(health.history.length)} samples kept).
            </Text>
          )}
        </Spacing>
      </Spacing>
    </SettingsDashboard>
  );
}

ServerHealthPage.getInitialProps = async () => ({});

export default PrivateRoute(ServerHealthPage);
