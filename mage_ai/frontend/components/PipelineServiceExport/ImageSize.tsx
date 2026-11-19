import styled from 'styled-components';

import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { isLightTheme } from '@oracle/styles/themes/mode';

// export.measure_image: a built image's size, in all and by part, from docker history.
export type ImagePartType = { bytes: number; key: string; label: string };
export type ImageBuildType = {
  built_at?: string;
  bytes: number;
  id?: string;
  image: string;
  parts: ImagePartType[];
  platform?: string;
};

// The categorical slots in their fixed order, light and dark steps (dataviz palette): a
// part keeps its color from one build to the next. The base image is neutral gray: it is
// what the service builds on, not something it adds.
const SLOTS = {
  dark: ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767'],
  light: ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#6250d6', '#e34948'],
};
const BASE = { dark: '#6f6f6b', light: '#a8a7a0' };

const BarStyle = styled.div<{ thin?: boolean }>`
  display: flex;
  gap: 2px;
  height: ${props => props.thin ? UNIT : UNIT * 2.5}px;
  width: 100%;
`;

const SegmentStyle = styled.div`
  min-width: 2px;

  &:first-child {
    border-bottom-left-radius: 4px;
    border-top-left-radius: 4px;
  }

  &:last-child {
    border-bottom-right-radius: 4px;
    border-top-right-radius: 4px;
  }
`;

const BarsStyle = styled.div`
  align-items: center;
  display: grid;
  gap: ${UNIT}px ${UNIT * 1.5}px;
  grid-template-columns: ${UNIT * 11}px 1fr;
`;

const RowsStyle = styled.table`
  border-collapse: collapse;
  max-width: ${UNIT * 70}px;
  width: 100%;

  td {
    padding: ${UNIT * 0.5}px ${UNIT}px ${UNIT * 0.5}px 0;
  }

  td:nth-child(n+3) {
    font-family: ${MONO_FONT_FAMILY_REGULAR};
    font-size: 12px;
    text-align: right;
    white-space: nowrap;
  }

  ${props => `
    tr + tr td {
      border-top: 1px solid ${(props.theme.borders || dark.borders).light};
    }
  `}
`;

const SwatchStyle = styled.span`
  border-radius: 2px;
  display: inline-block;
  height: ${UNIT * 1.25}px;
  width: ${UNIT * 1.25}px;
`;

export function megabytes(bytes: number): string {
  if (bytes >= 1e9) {
    return `${(bytes / 1e9).toFixed(2)} GB`;
  }
  return bytes >= 1e6 ? `${Math.round(bytes / 1e6).toLocaleString()} MB`
    : `${Math.max(1, Math.round(bytes / 1e3))} kB`;
}

function change(now: number, before?: number): string {
  if (before === undefined || Math.abs(now - before) < 1e6) {
    return '';
  }
  return `${now > before ? '+' : '-'}${megabytes(Math.abs(now - before))}`;
}

function when(iso?: string): string {
  const date = iso ? new Date(iso) : null;
  return date && !Number.isNaN(date.getTime()) ? date.toLocaleString() : '';
}

// One bar of the image, a segment per part, its values in text beside the colors; the build
// before as a thin bar on the same scale, and each part's change since it.
function ImageSize({ latest, previous }: { latest: ImageBuildType; previous?: ImageBuildType }) {
  const mode = isLightTheme() ? 'light' : 'dark';
  // Colors by part, over both builds, so a part has the same color in both bars.
  const keys = [...latest.parts, ...(previous?.parts || [])].map(p => p.key)
    .filter((key, index, all) => all.indexOf(key) === index && key !== 'base');
  const color = (key: string) => key === 'base' ? BASE[mode]
    : SLOTS[mode][keys.indexOf(key) % SLOTS[mode].length];
  const scale = Math.max(latest.bytes, previous?.bytes || 0) || 1;
  const before = Object.fromEntries((previous?.parts || []).map(p => [p.key, p.bytes]));
  const total = change(latest.bytes, previous?.bytes);

  const bar = (build: ImageBuildType, thin?: boolean) => (
    <div style={{ width: `${(build.bytes / scale) * 100}%` }}>
      <BarStyle aria-hidden thin={thin}>
        {build.parts.map(part => (
          <SegmentStyle
            key={part.key}
            style={{ backgroundColor: color(part.key), flexGrow: part.bytes, opacity: thin ? 0.6 : 1 }}
            title={`${part.label}: ${megabytes(part.bytes)}`}
          />
        ))}
      </BarStyle>
    </div>
  );

  return (
    <div>
      <Text bold>
        {latest.image} is {megabytes(latest.bytes)}
        {total ? `, ${total} since the build before` : ''}
      </Text>
      <Text muted small>
        {[latest.platform, when(latest.built_at)].filter(Boolean).join(' · ')}
      </Text>
      <Spacing mt={1.5}>
        <BarsStyle>
          <Text muted small>This build</Text>
          {bar(latest)}
          {previous && (
            <>
              <Text muted small title={when(previous.built_at)}>
                Before: {megabytes(previous.bytes)}
              </Text>
              {bar(previous, true)}
            </>
          )}
        </BarsStyle>
      </Spacing>
      <Spacing mt={1.5}>
        <RowsStyle aria-label="Image size by part">
          <tbody>
            {latest.parts.map(part => (
              <tr key={part.key}>
                <td style={{ width: UNIT * 2 }}>
                  <SwatchStyle style={{ backgroundColor: color(part.key) }} />
                </td>
                <td><Text small>{part.label}</Text></td>
                <td><Text inline small>{megabytes(part.bytes)}</Text></td>
                <td>
                  <Text inline muted small>
                    {previous ? change(part.bytes, before[part.key] || 0) : ''}
                  </Text>
                </td>
              </tr>
            ))}
            {(previous?.parts || []).filter(p => !latest.parts.some(l => l.key === p.key)).map(
              part => (
                <tr key={part.key}>
                  <td />
                  <td><Text muted small>{part.label} (gone)</Text></td>
                  <td><Text inline muted small>0</Text></td>
                  <td><Text inline muted small>{change(0, part.bytes)}</Text></td>
                </tr>
              ),
            )}
          </tbody>
        </RowsStyle>
      </Spacing>
    </div>
  );
}

export default ImageSize;
