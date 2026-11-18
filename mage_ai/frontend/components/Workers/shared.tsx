import styled from 'styled-components';

import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS, BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { FONT_FAMILY_MEDIUM, MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { PackageChangeType, WorkerSecurityType, WorkerType, WorkerUsageType } from './types';
import { UNIT } from '@oracle/styles/units/spacing';

export type ToneType = 'positive' | 'warning' | 'negative' | 'info' | 'muted';

function toneColor(tone: ToneType, theme): string {
  const accent = (theme.accent || dark.accent);
  return {
    info: accent.sky,
    muted: (theme.content || dark.content).muted,
    negative: accent.negative,
    positive: accent.positive,
    warning: accent.warning,
  }[tone];
}

// A state in a word or two, its color the tone's; the dot keeps it readable without color.
export const PillStyle = styled.span<{ tone: ToneType }>`
  align-items: center;
  border-radius: ${UNIT * 1.5}px;
  display: inline-flex;
  font-family: ${FONT_FAMILY_MEDIUM};
  font-size: 12px;
  gap: ${UNIT * 0.75}px;
  line-height: 20px;
  padding: 0 ${UNIT}px;
  white-space: nowrap;

  ${props => `
    border: 1px solid ${toneColor(props.tone, props.theme)};
    color: ${toneColor(props.tone, props.theme)};
  `}

  &::before {
    border-radius: 50%;
    content: '';
    height: 6px;
    width: 6px;
    ${props => `background-color: ${toneColor(props.tone, props.theme)};`}
  }
`;

export const CardStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  padding: ${UNIT * 2}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

export const TableStyle = styled.table`
  border-collapse: collapse;
  width: 100%;

  th {
    font-family: ${FONT_FAMILY_MEDIUM};
    font-size: 12px;
    font-weight: normal;
    padding: 0 ${UNIT * 1.5}px ${UNIT}px 0;
    text-align: left;
    white-space: nowrap;
    ${props => `color: ${(props.theme.content || dark.content).muted};`}
  }

  td {
    padding: ${UNIT * 1.25}px ${UNIT * 1.5}px ${UNIT * 1.25}px 0;
    vertical-align: middle;
    ${props => `border-top: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }
`;

export const RowLinkStyle = styled.tr`
  cursor: pointer;

  &:hover td, &:focus-visible td {
    ${props => `background-color: ${(props.theme.background || dark.background).popup};`}
  }

  &:focus-visible {
    outline: none;
  }
`;

export const MonoStyle = styled.span`
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  // What people typed, as typed: no ">=" drawn as one sign.
  font-variant-ligatures: none;
  ${props => `color: ${(props.theme.content || dark.content).default};`}
`;

// A share of the largest value, as a thin bar beside a number.
export const BarStyle = styled.div<{ share: number }>`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  height: 6px;
  min-width: 2px;
  width: ${props => Math.max(0, Math.min(1, props.share)) * 100}%;

  ${props => `background-color: ${(props.theme.accent || dark.accent).sky};`}
`;

export function formatBytes(bytes?: number): string {
  if (bytes === undefined || bytes === null) {
    return '-';
  }
  if (bytes >= 1e9) {
    return `${(bytes / 1e9).toFixed(1)} GB`;
  }
  if (bytes >= 1e6) {
    return `${Math.round(bytes / 1e6)} MB`;
  }
  return `${Math.max(1, Math.round(bytes / 1e3))} KB`;
}

export function plural(count: number, word: string): string {
  return `${count} ${word}${count === 1 ? '' : 's'}`;
}

export function usageText(usedBy?: WorkerUsageType[]): string {
  if (!usedBy?.length) {
    return 'Not used yet';
  }
  const blocks = usedBy.reduce((sum, u) => sum + u.blocks.length, 0);
  return `${plural(blocks, 'block')} in ${plural(usedBy.length, 'pipeline')}`;
}

// Whether a worker can run blocks, said once for the list and the page.
export function workerState(worker: WorkerType): { label: string; tone: ToneType; hint: string } {
  if (worker.job?.state === 'running') {
    return { hint: 'uv is installing its packages.', label: 'Preparing', tone: 'info' };
  }
  if (!worker.locked) {
    return {
      hint: 'It has no uv.lock: save its packages to lock them.',
      label: 'Not locked',
      tone: 'negative',
    };
  }
  if (worker.job?.state === 'failed' && !worker.prepared) {
    return { hint: worker.job?.problems?.[0] || 'Preparing failed.', label: 'Failed', tone: 'negative' };
  }
  if (!worker.prepared) {
    return {
      hint: 'Its blocks fail until it is prepared.',
      label: 'Not prepared',
      tone: 'warning',
    };
  }
  return { hint: 'Its packages are installed on this server.', label: 'Ready', tone: 'positive' };
}

export function securityState(security?: WorkerSecurityType): {
  label: string;
  tone: ToneType;
} {
  if (!security || security.state === 'unchecked') {
    return { label: 'Not checked', tone: 'muted' };
  }
  if (security.state === 'clean') {
    return { label: 'No known issues', tone: 'positive' };
  }
  const count = security.advisories || 0;
  return {
    label: `${count} ${count === 1 ? 'vulnerability' : 'vulnerabilities'}`,
    tone: 'negative',
  };
}

export function needsAttention(worker: WorkerType): boolean {
  return !worker.prepared || worker.security?.state === 'vulnerable';
}

function major(version?: string): string {
  return (version || '').split(/[.,]/)[0];
}

// The packages a change moves: added, removed, or from one version to another; a new major
// version stands out, as it may change behavior.
export function ChangesList({ changes }: { changes: PackageChangeType[] }) {
  return (
    <TableStyle>
      <thead>
        <tr>
          <th>Package</th>
          <th>Now</th>
          <th>After</th>
        </tr>
      </thead>
      <tbody>
        {changes.map(change => {
          const bigger = change.from && change.to && major(change.from) !== major(change.to);
          return (
            <tr key={change.package}>
              <td><MonoStyle>{change.package}</MonoStyle></td>
              <td><Text muted={!change.from} small>{change.from || 'Not installed'}</Text></td>
              <td>
                <Text small warning={!!bigger}>
                  {change.to || 'Removed'}
                  {bigger ? ' · new major version' : ''}
                </Text>
              </td>
            </tr>
          );
        })}
      </tbody>
    </TableStyle>
  );
}
