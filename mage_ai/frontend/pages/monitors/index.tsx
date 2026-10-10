import Dashboard from '@components/Dashboard';
import Monitors from '@components/Monitors';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function MonitorsPage() {
  return (
    <Dashboard
      title="Monitors"
      uuid="Monitors/index"
    >
      <Spacing p={PADDING_UNITS}>
        <Monitors />
      </Spacing>
    </Dashboard>
  );
}

MonitorsPage.getInitialProps = async () => ({});

export default PrivateRoute(MonitorsPage);
