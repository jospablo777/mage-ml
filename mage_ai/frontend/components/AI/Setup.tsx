import Link from '@oracle/elements/Link';
import Panel from '@oracle/components/Panel';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import {
  UNITS_BETWEEN_SECTIONS
} from '@oracle/styles/units/spacing';

export default function Setup() {
  return (
    <Spacing mb={UNITS_BETWEEN_SECTIONS}>
      <Panel>
        <Text warning>
          You need to add an OpenAI API key to your project before you can
          generate pipelines using AI.
        </Text>

        <Spacing mt={1}>
          <Text warning>
            Read <Link
              href="https://help.openai.com/en/articles/4936850-where-do-i-find-my-secret-api-key"
              openNewWindow
            >
              OpenAI’s documentation
            </Link> to get your API key.
          </Text>
        </Spacing>

      </Panel>
    </Spacing >
  )
}
