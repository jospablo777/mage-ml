import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS } from '@oracle/styles/units/borders';
import { HeldPackageType } from './types';
import { UNIT } from '@oracle/styles/units/spacing';

const CalloutStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  padding: ${UNIT * 1.5}px ${UNIT * 2}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    border: 1px solid ${(props.theme.accent || dark.accent).warning};
  `}
`;

function headline(packages: HeldPackageType[]): string {
  if (packages.length === 1) {
    const [p] = packages;
    return `uv would install ${p.package} ${p.locked}, not the newest ${p.latest}`;
  }
  return `uv would install older releases of ${packages.map(p => p.package).join(', ')}`;
}

// A save or a new worker that uv locks with a package below its newest release, because
// the newest does not fit the other packages: nothing is written until people say so.
// Changing the versions is the default; going on keeps the old release.
function OlderReleases({ accepting, onAccept, onCancel, packages }: {
  accepting?: boolean;
  onAccept: () => void;
  onCancel: () => void;
  packages: HeldPackageType[];
}) {
  const kept = packages.map(p => `${p.package} ${p.locked}`).join(', ');

  return (
    <CalloutStyle aria-label="Older releases" role="alertdialog">
      <Text bold>{headline(packages)}</Text>
      {packages.map(p => (
        <Spacing key={p.package} mt={1}>
          <Text default small>{p.question || p.reason}</Text>
        </Spacing>
      ))}
      <Spacing mt={1}>
        <Text muted xsmall>
          An old release can list ranges written before today&apos;s versions existed: it
          locks, and its code may still fail with them. Nothing changed yet.
        </Text>
      </Spacing>
      <Spacing mt={1.5}>
        <FlexContainer alignItems="center">
          <Button onClick={onCancel} primary>Change the versions</Button>
          <Spacing ml={1} />
          <Button loading={accepting} onClick={onAccept}>Go on with {kept}</Button>
        </FlexContainer>
      </Spacing>
    </CalloutStyle>
  );
}

export default OlderReleases;
