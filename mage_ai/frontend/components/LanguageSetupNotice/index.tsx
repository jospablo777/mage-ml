import { useState } from 'react';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import Checkbox from '@oracle/elements/Checkbox';
import CopyToClipboard from '@oracle/components/CopyToClipboard';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { AlertTriangle } from '@oracle/icons';
import { BORDER_RADIUS, BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';

// What GET /api/language_setups/<language> answers.
export type LanguageSetupType = {
  commands: string[];
  guide: string;
  install: string;
  language: 'r' | 'rust';
  name: string;
  problems: string[];
  ready: boolean;
};

const HIDDEN_KEY = 'mage_language_setup_notice_hidden_';

export function noticeHidden(language: string): boolean {
  try {
    return typeof window !== 'undefined'
      && window.localStorage.getItem(`${HIDDEN_KEY}${language}`) === '1';
  } catch {
    return false;
  }
}

function hideNotice(language: string, hidden: boolean) {
  try {
    if (hidden) {
      window.localStorage.setItem(`${HIDDEN_KEY}${language}`, '1');
    } else {
      window.localStorage.removeItem(`${HIDDEN_KEY}${language}`);
    }
  } catch {
    // Without storage the notice shows again next time, which is harmless.
  }
}

const CardStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  max-width: calc(100vw - ${UNIT * 4}px);
  padding: ${UNIT * 3}px;
  width: ${UNIT * 78}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).medium};
    box-shadow: ${(props.theme.shadow || dark.shadow).popup};
  `}
`;

const CommandStyle = styled.pre`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 13px;
  line-height: 1.6;
  margin: 0;
  overflow-x: auto;
  padding: ${UNIT * 1.5}px;
  white-space: pre;

  ${props => `
    background-color: ${(props.theme.background || dark.background).codeArea};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

// Shown when someone creates an R or Rust block and that language is not set up here: the
// block is created, and this says what to install to run it.
function LanguageSetupNotice({
  onClose,
  setup,
}: {
  onClose: () => void;
  setup: LanguageSetupType;
}) {
  const [dontShow, setDontShow] = useState<boolean>(false);
  const commands = setup.commands.join('\n');

  return (
    <CardStyle role="dialog" aria-label={`Set up ${setup.name} to run ${setup.name} blocks`}>
      <FlexContainer alignItems="center">
        <AlertTriangle size={UNIT * 2.5} warning />
        <Spacing ml={1} />
        <Text bold large>Set up {setup.name} to run {setup.name} blocks</Text>
      </FlexContainer>

      <Spacing mt={1.5}>
        <Text default>
          The block is created. MageML runs Python blocks out of the box; {setup.name} is
          installed outside Python, so this machine needs it before the block can run.
        </Text>
      </Spacing>

      {setup.problems.map(problem => (
        <Spacing key={problem} mt={1.5}>
          <Text muted small>{problem}</Text>
        </Spacing>
      ))}

      <Spacing mt={2}>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <Text bold small>In a terminal</Text>
          <CopyToClipboard copiedText={commands} linkText="Copy" small withCopyIcon />
        </FlexContainer>
        <Spacing mt={0.75}><CommandStyle>{commands}</CommandStyle></Spacing>
      </Spacing>

      <Spacing mt={2}>
        <Text muted small>
          For databases and other integrations, install MageML with its extras, such as:
        </Text>
        <Spacing mt={0.75}><CommandStyle>{setup.install}</CommandStyle></Spacing>
        <Spacing mt={0.75}>
          <Text muted xsmall>
            The r and rust extras add no packages: {setup.name} itself comes from the commands
            above.
          </Text>
        </Spacing>
      </Spacing>

      <Spacing mt={3}>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <Checkbox
            checked={dontShow}
            label={`Don't show this again for ${setup.name}`}
            onClick={() => setDontShow(prev => !prev)}
          />
          <FlexContainer alignItems="center">
            <a
              href={setup.guide}
              rel="noreferrer"
              style={{ textDecoration: 'underline' }}
              target="_blank"
            >
              <Text default small>{setup.name} setup guide</Text>
            </a>
            <Spacing ml={2} />
            <Button
              onClick={() => {
                hideNotice(setup.language, dontShow);
                onClose();
              }}
              primary
            >
              Got it
            </Button>
          </FlexContainer>
        </FlexContainer>
      </Spacing>
    </CardStyle>
  );
}

export default LanguageSetupNotice;
