import styled from 'styled-components';
import { useState } from 'react';

import Link from '@oracle/elements/Link';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS } from '@oracle/styles/units/borders';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';

const CalloutStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  padding: ${UNIT * 1.5}px ${UNIT * 2}px;

  ${props => `
    background-color: ${(props.theme.accent || dark.accent).negativeTransparent};
    border: 1px solid ${(props.theme.accent || dark.accent).negative};
  `}
`;

const OutputStyle = styled.pre`
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 1.6;
  margin: ${UNIT}px 0 0;
  max-height: ${UNIT * 40}px;
  overflow: auto;
  white-space: pre-wrap;

  ${props => `color: ${(props.theme.content || dark.content).default};`}
`;

// What went wrong, said first; the tool's own output (uv's unless named, after a blank line
// in the message) one click away.
function ErrorCallout({ message, title, tool = 'uv' }: {
  message: string;
  title: string;
  tool?: string;
}) {
  const [open, setOpen] = useState(false);
  const [summary, ...rest] = (message || '').split('\n\n');
  const output = rest.join('\n\n').trim();

  return (
    <CalloutStyle role="alert">
      <Text bold>{title}</Text>
      <Spacing mt={1} />
      <Text default>{summary}</Text>
      {output && (
        <Spacing mt={1}>
          <Link onClick={() => setOpen(!open)} small>
            {open ? `Hide ${tool}'s output` : `Show ${tool}'s output`}
          </Link>
          {open && <OutputStyle>{output}</OutputStyle>}
        </Spacing>
      )}
    </CalloutStyle>
  );
}

export default ErrorCallout;
