import Dashboard from '@components/Dashboard';
import Incidents from '@components/Incidents';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function IncidentsPage() {
  return (
    <Dashboard
      title="Incidents"
      uuid="Incidents/index"
    >
      <Spacing p={PADDING_UNITS}>
        <Incidents />
      </Spacing>
    </Dashboard>
  );
}

IncidentsPage.getInitialProps = async () => ({});

export default PrivateRoute(IncidentsPage);
