import Dashboard from '@components/Dashboard';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import Workers from '@components/Workers';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function WorkersPage() {
  return (
    <Dashboard
      title="Workers"
      uuid="Workers/index"
    >
      <Spacing p={PADDING_UNITS}>
        <Workers />
      </Spacing>
    </Dashboard>
  );
}

WorkersPage.getInitialProps = async () => ({});

export default PrivateRoute(WorkersPage);
