import Dashboard from '@components/Dashboard';
import LanguageEnvironment from '@components/Workers/LanguageEnvironment';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function LanguageEnvironmentPage({ language }: { language: string }) {
  return (
    <Dashboard
      title={(language || '').startsWith('rust') ? 'Rust' : 'R'}
      uuid="Workers/language"
    >
      <Spacing p={PADDING_UNITS}>
        <LanguageEnvironment language={language} />
      </Spacing>
    </Dashboard>
  );
}

LanguageEnvironmentPage.getInitialProps = async (ctx: any) => ({
  language: ctx.query.language,
});

export default PrivateRoute(LanguageEnvironmentPage);
